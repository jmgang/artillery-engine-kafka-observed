import { parseAuthMode, type KafkaAuthMode } from './connection.js';

/** config.kafkaObserved after normalisation. */
export interface EngineOptions {
  brokers:string[];topic:string;group:string;runId:string;partitions:number;reportDirectory:string;
  observationIntervalMs:number;drainTimeoutMs:number;auth:KafkaAuthMode;region?:string;
  acks?:unknown;idempotent?:unknown;hotPartitionShare?:unknown;hotPartition?:unknown;
}

/**
 * Validates config.kafkaObserved. Values may be typed or strings rendered by Artillery from
 * `{{ $env.NAME }}` (brokers as a comma-separated list, numbers as text). An unset variable renders
 * empty or as the literal template and is rejected with the field name, so a run never starts half-configured.
 */
export function engineOptions(raw:Record<string,unknown>|undefined):EngineOptions{
  if(!raw||typeof raw!=='object')throw new Error('config.kafkaObserved is required');
  const text=(field:string,required=true):string=>{
    const v=raw[field];const s=v===undefined||v===null?'':String(v).trim();
    if(s.includes('{{')||s==='undefined')throw new Error(`kafkaObserved.${field} is an unresolved template; set its environment variable`);
    if(required&&!s)throw new Error(`kafkaObserved.${field} is required`);
    return s;
  };
  const integer=(field:string,fallback:number,min:number):number=>{
    const s=text(field,false);const n=s?Number(s):fallback;
    if(!Number.isInteger(n)||n<min)throw new Error(`kafkaObserved.${field} must be an integer >= ${min}`);
    return n;
  };
  const brokerList=Array.isArray(raw.brokers)?raw.brokers.map(String):text('brokers').split(',');
  const brokers=brokerList.map(b=>b.trim()).filter(Boolean);
  if(!brokers.length||brokers.some(b=>b.includes('{{')||b==='undefined'))throw new Error('kafkaObserved.brokers must list at least one broker (e.g. KAFKA_BOOTSTRAP)');
  const runId=text('runId');
  if(!/^[a-zA-Z0-9-]{1,50}$/.test(runId))throw new Error('kafkaObserved.runId must be 1..50 of [a-zA-Z0-9-]');
  const region=text('region',false);
  return {brokers,topic:text('topic'),group:text('group'),runId,partitions:integer('partitions',0,1),
    reportDirectory:text('reportDirectory'),observationIntervalMs:integer('observationIntervalMs',1000,100),
    drainTimeoutMs:integer('drainTimeoutMs',45000,0),auth:parseAuthMode(raw.auth),...(region?{region}:{}),
    acks:raw.acks,idempotent:raw.idempotent,hotPartitionShare:raw.hotPartitionShare,hotPartition:raw.hotPartition};
}
