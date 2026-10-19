import { performance } from 'node:perf_hooks';
import type { KafkaAuthMode } from './connection.js';
import { KafkaHostClient, type KafkaAdmin } from './kafka-client.js';
import { hotPartitionFor, DEFAULT_PRODUCER_SETTINGS, type ProducerSettings } from './producer-settings.js';

export interface PublishMetrics { sent:number;failed:number;deliveryUnknown:number;sendLatencyCount:number;sendLatencySumMs:number;sendLatencyMaxMs:number; }

/**
 * One shared producer and one Admin client per engine (never one per virtual user). Each runs in its own Kafka
 * host process (@confluentinc/kafka-javascript, librdkafka), so an Admin fault never stops production.
 */
export class Publisher {
  private readonly producerHost:KafkaHostClient;
  private readonly adminHost:KafkaHostClient;
  readonly admin:KafkaAdmin;
  readonly metrics:PublishMetrics={sent:0,failed:0,deliveryUnknown:0,sendLatencyCount:0,sendLatencySumMs:0,sendLatencyMaxMs:0};
  constructor(brokers:string[],clientId:string,private observe?:(kind:'counter'|'histogram'|'rate',name:string,value:number)=>void,
      connection:{auth?:KafkaAuthMode;region?:string}={},readonly settings:ProducerSettings=DEFAULT_PRODUCER_SETTINGS,private partitions=0){
    this.producerHost=new KafkaHostClient('producer',{brokers,clientId,...connection},settings);
    this.adminHost=new KafkaHostClient('admin',{brokers,clientId:`${clientId}-admin`,...connection},settings);
    this.admin=this.adminHost.admin();
  }
  async connect():Promise<void>{ await Promise.all([this.adminHost.connect(),this.producerHost.connect()]); }
  /** Host restarts after unexpected exits (a native-client crash); reported in the engine report. */
  get hostRestarts():{producer:number;admin:number}{ return {producer:this.producerHost.restarts,admin:this.adminHost.restarts}; }
  /** Resolves with the acknowledged partition and offset (offset is null with acks=0). */
  async send(topic:string,key:string,value:Buffer):Promise<{partition:number;offset:string|null}>{
    if(value.length>65536)throw new Error('Payload exceeds 64 KiB contract');
    const start=performance.now();
    try{const ack=await this.producerHost.send(topic,key,value,this.partitions>0?hotPartitionFor(key,this.settings,this.partitions):undefined);
      this.metrics.sent++;this.observe?.('counter','kafka.messages_sent',1);this.observe?.('rate','kafka.send_rate',1);return ack;}
    catch(error){this.metrics.failed++;this.metrics.deliveryUnknown++;
      this.observe?.('counter','kafka.messages_failed',1);this.observe?.('counter','kafka.messages_delivery_unknown',1);throw error;}
    finally{const elapsed=performance.now()-start;this.metrics.sendLatencyCount++;this.metrics.sendLatencySumMs+=elapsed;
      this.metrics.sendLatencyMaxMs=Math.max(this.metrics.sendLatencyMaxMs,elapsed);this.observe?.('histogram','kafka.send_latency_ms',elapsed);}
  }
  async close():Promise<void>{ await Promise.allSettled([this.producerHost.close(),this.adminHost.close()]); }
}
