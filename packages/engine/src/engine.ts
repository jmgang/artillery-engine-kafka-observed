import { EventEmitter } from 'node:events';
import { performance,monitorEventLoopDelay } from 'node:perf_hooks';
import { mkdirSync,appendFileSync,writeFileSync,createWriteStream,type WriteStream } from 'node:fs';
import { resolve } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { Publisher } from './publisher.js';
import { engineOptions, type EngineOptions } from './options.js';
import { Observer, type Sample } from './observer.js';
import { producerSettings, type ProducerSettings } from './producer-settings.js';

/** id (optional): the business event id, recorded with the Kafka acknowledgement so verifiers can count lost events. */
interface Payload { key:string;value:Buffer|string;id?:string; }
interface PayloadInput { ordinal:number;scheduledAt:Date;vars:Record<string,unknown>; }
interface Script {config:{kafkaObserved:Record<string,unknown>;processor:Record<string,(input:PayloadInput)=>Payload>;
  /** Optional: what the experiment expects (title, hypothesis, checks), saved as experiment.json for the verifier. */
  experiment?:Record<string,unknown>;phases?:unknown[]};}
interface Scenario {flow:{publish:{factory:string}}[];}
type MetricsEE=Pick<EventEmitter,'emit'>;

/** Generic publishing engine; payload factories own business fixture generation. */
class KafkaObservedEngine {
  static sessions=new Set<KafkaObservedEngine>();
  private options:EngineOptions;private publisher:Publisher;private observer:Observer;
  private metricEvents?:MetricsEE;private ordinal=0;private active=0;private phaseStart=0;private phaseOrdinal=0;private artilleryPhase:{name:string;arrivalRate?:number;rampTo?:number}|null=null;private intervalMs=0;
  private timer?:NodeJS.Timeout;private busy=false;private closing?:Promise<void>;
  private cpu=process.cpuUsage();private sampledAt=performance.now();
  private eventLoop=monitorEventLoopDelay({resolution:20});private sampleFile:string;
  /** One line per acknowledged event and per failed/unknown delivery. */
  private published:WriteStream;private publishFailures:WriteStream;
  constructor(private script:Script,events:EventEmitter){
    this.options=engineOptions(script.config.kafkaObserved);
    const o=this.options;
    mkdirSync(o.reportDirectory,{recursive:true});this.sampleFile=resolve(o.reportDirectory,'engine-samples.jsonl');
    this.published=createWriteStream(resolve(o.reportDirectory,'published.jsonl'),{flags:'a'});
    this.publishFailures=createWriteStream(resolve(o.reportDirectory,'publish-failures.jsonl'),{flags:'a'});
    if(script.config.experiment)writeFileSync(resolve(o.reportDirectory,'experiment.json'),
      JSON.stringify({...script.config.experiment,phases:script.config.phases??[]},null,2)+'\n');
    const settings:ProducerSettings=producerSettings({acks:o.acks,idempotent:o.idempotent,hotPartitionShare:o.hotPartitionShare,hotPartition:o.hotPartition});
    this.publisher=new Publisher(o.brokers,`artillery-${o.runId}`,(kind,name,value)=>this.metricEvents?.emit(kind,name,value),
      {auth:o.auth,region:o.region},settings,o.partitions);
    this.observer=new Observer(this.publisher.admin,o.topic,o.group,'earliest');
    events.on('phaseStarted',(phase:{name?:string;index?:number;arrivalCount?:number;duration?:number;arrivalRate?:number;rampTo?:number})=>{
      this.phaseStart=Date.now();this.phaseOrdinal=0;
      this.artilleryPhase={name:phase.name??`phase ${phase.index??0}`,arrivalRate:phase.arrivalRate,rampTo:phase.rampTo};
      this.intervalMs=phase.arrivalCount&&phase.duration?phase.duration*1000/phase.arrivalCount:
        phase.arrivalRate&&!phase.rampTo?1000/phase.arrivalRate:0;
      if(this.intervalMs)this.metricEvents?.emit('histogram','load.requested_rate',1000/this.intervalMs);
    });
    KafkaObservedEngine.sessions.add(this);
  }
  async init():Promise<void>{await this.publisher.connect();this.eventLoop.enable();
    this.timer=setInterval(()=>{void this.poll();},this.options.observationIntervalMs);}
  createScenario(spec:Scenario,events:MetricsEE):(context:{vars:Record<string,unknown>},callback:(error:Error|null,context:unknown)=>void)=>void{
    this.metricEvents=events;
    if(!spec.flow?.length||spec.flow.some(action=>!action.publish||typeof this.script.config.processor?.[action.publish.factory]!=='function'))
      throw new Error('Each action must name an existing synchronous processor payload factory');
    return (context,callback)=>{
      events.emit('started');this.active++;
      if(this.intervalMs)events.emit('histogram','load.requested_rate',1000/this.intervalMs);
      // Arrival is driven by Artillery, independent of the consumer's processing speed.
      const planned=this.intervalMs?this.phaseStart+(++this.phaseOrdinal)*this.intervalMs:Date.now();
      void (async()=>{
        for(const action of spec.flow){
          const generated=Date.now();events.emit('histogram','load.schedule_delay_ms',Math.max(0,generated-planned));
          const payload=this.script.config.processor[action.publish.factory]!({ordinal:this.ordinal++,scheduledAt:new Date(planned),vars:context.vars});
          if(typeof payload?.key!=='string'||(!Buffer.isBuffer(payload.value)&&typeof payload.value!=='string'))throw new Error('Payload factory must return a key and Buffer/string value');
          events.emit('rate','load.actual_rate',1);
          events.emit('histogram','load.generator.publish_batch_size',1);
          try{
            const ack=await this.publisher.send(this.options.topic,payload.key,Buffer.isBuffer(payload.value)?payload.value:Buffer.from(payload.value));
            this.published.write(JSON.stringify({id:payload.id??null,key:payload.key,partition:ack.partition,offset:ack.offset,ackedAt:new Date().toISOString()})+'\n');
          }catch(error){
            // Delivery unknown: the broker may or may not have the record; verifiers report these separately.
            this.publishFailures.write(JSON.stringify({id:payload.id??null,key:payload.key,failedAt:new Date().toISOString(),
              error:error instanceof Error?error.message.slice(0,200):'publish failed'})+'\n');
            throw error;
          }
        }
      })().then(()=>{this.active--;callback(null,context);},error=>{this.active--;callback(error instanceof Error?error:new Error('Kafka publish failed'),context);});
    };
  }
  private async poll():Promise<Sample|undefined>{
    if(this.busy)return;this.busy=true;
    try{
      const sample=await this.observer.sample();
      appendFileSync(this.sampleFile,JSON.stringify({...sample,phase:this.closing?'drain':'generation',artilleryPhase:this.closing?null:this.artilleryPhase})+'\n');
      const gauges:Record<string,number|null>={'kafka.lag.total':safeOffset(sample.lagTotal),'kafka.lag.max':safeOffset(sample.lagMax),
        'kafka.lag.avg':sample.lagAvg,'kafka.partitions_observed':sample.partitionsObserved,
        'kafka.partitions_uninitialized':sample.partitionsUninitialized,'kafka.production_rate':sample.productionRate,
        'kafka.committed_rate':sample.committedRate,'kafka.metrics_age_seconds':sample.ageSeconds};
      for(const [name,value] of Object.entries(gauges))if(value!==null)this.metricEvents?.emit('histogram',name,value);
      return sample;
    }catch(error){
      const status=this.observer.status();this.metricEvents?.emit('counter','kafka.metrics_errors',1);
      if(status.ageSeconds!==null)this.metricEvents?.emit('histogram','kafka.metrics_age_seconds',status.ageSeconds);
      appendFileSync(this.sampleFile,JSON.stringify({timestamp:new Date().toISOString(),observationFailed:true,...status})+'\n');
    }finally{
      const now=performance.now(),elapsed=(now-this.sampledAt)*1000;const cpu=process.cpuUsage(this.cpu);this.cpu=process.cpuUsage();this.sampledAt=now;
      this.metricEvents?.emit('histogram','load.generator.cpu_percent',(cpu.user+cpu.system)/elapsed*100);
      this.metricEvents?.emit('histogram','load.generator.rss_bytes',process.memoryUsage().rss);
      if(Number.isFinite(this.eventLoop.mean))this.metricEvents?.emit('histogram','load.generator.event_loop_delay_ms',this.eventLoop.mean/1e6);
      this.eventLoop.reset();this.busy=false;
    }
  }
  finish():Promise<void>{
    if(this.closing)return this.closing;
    this.closing=(async()=>{
      if(this.timer)clearInterval(this.timer);
      const deadline=Date.now()+this.options.drainTimeoutMs;let drained=false;
      try{
        while(Date.now()<deadline){const sample=await this.poll();
          if(this.active===0&&sample?.partitionsObserved===this.options.partitions&&sample.lagTotal==='0'){drained=true;break;}
          await sleep(250);}
        writeFileSync(resolve(this.options.reportDirectory,'engine-report.json'),JSON.stringify({runId:this.options.runId,producerSettings:this.publisher.settings,kafkaHostRestarts:this.publisher.hostRestarts,...this.publisher.metrics,
          drained,offsetStartPolicy:'earliest',sampledGauges:'Artillery histograms; time series retained separately',
          committedProgressIsNotApplicationSuccess:true,drainSamplesOutsideArtilleryFinalReport:true},null,2));
        if(!drained)throw new Error('Kafka observation/drain gate did not pass');
      }finally{this.eventLoop.disable();await this.publisher.close();
        await Promise.all([this.published,this.publishFailures].map(stream=>new Promise<void>(done=>stream.end(done))));
        KafkaObservedEngine.sessions.delete(this);}
    })();return this.closing;
  }
  static async finishAll():Promise<void>{await Promise.all([...this.sessions].map(session=>session.finish()));}
}
function safeOffset(value:string):number|null{const parsed=BigInt(value);return parsed<=BigInt(Number.MAX_SAFE_INTEGER)?Number(parsed):null;}
export=KafkaObservedEngine;
