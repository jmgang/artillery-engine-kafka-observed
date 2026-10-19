/**
 * Helper process that owns one @confluentinc/kafka-javascript client: the producer or the Admin client.
 *
 * Why a separate process: Artillery runs engines inside worker threads, and the librdkafka native addon
 * delivers its callbacks on the process's main thread, which aborts Node when the client lives in a worker.
 * The engine starts its hosts itself (kafka-client.ts) and talks to them over Node's IPC channel; users of the
 * engine never see them. Producer and Admin run in separate hosts so a fault in one never stops the other.
 * A host exits when the engine disconnects or its process ends.
 */
import { kafkaConfig, type ConnectionOptions } from './connection.js';
import { ackValue, type ProducerSettings } from './producer-settings.js';
import type { KafkaJS } from '@confluentinc/kafka-javascript';

export type HostRole='producer'|'admin';
export type AdminMethod='fetchTopicOffsets'|'fetchOffsets'|'createTopics'|'deleteTopics';
export type HostRequest=
  | {id:number;op:'connect';role:HostRole;connection:ConnectionOptions;settings:ProducerSettings}
  | {id:number;op:'send';topic:string;key:string;value:Uint8Array;partition?:number}
  | {id:number;op:'admin';method:AdminMethod;args:unknown[]}
  | {id:number;op:'close'};
export type HostReply={id:number;ok:true;result:unknown}|{id:number;ok:false;error:string};

/** Admin reads used for lag observation: short, so a dead broker costs one sample, not many. */
const ADMIN_TIMEOUT_MS=3000;

let producer:KafkaJS.Producer|undefined;
let admin:KafkaJS.Admin|undefined;

/** Admin results can hold class instances and bigints; send plain data over the IPC channel. */
function plain(value:unknown):unknown{
  return value===undefined?null:JSON.parse(JSON.stringify(value,(_key,v)=>typeof v==='bigint'?v.toString():v));
}

async function handle(request:HostRequest):Promise<unknown>{
  if(request.op==='connect'){
    const confluent=require('@confluentinc/kafka-javascript') as typeof import('@confluentinc/kafka-javascript');
    const kafka=new confluent.KafkaJS.Kafka(kafkaConfig(request.connection));
    if(request.role==='admin'){
      // Refresh cluster metadata every 2 s (default 5 min): after a broker dies, offset reads follow the new
      // partition leaders within seconds instead of timing out against the dead broker until the next refresh.
      admin=kafka.admin({'topic.metadata.refresh.interval.ms':2000,'metadata.max.age.ms':6000});
      await admin.connect();
      return null;
    }
    const s=request.settings;
    producer=kafka.producer({
      // The Java client's default key placement, so keys land on the same partitions as with Kafka's own clients.
      partitioner:'murmur2_random',
      // Bounded delivery: a send the cluster cannot accept fails (delivery unknown) instead of waiting 5 minutes.
      'message.timeout.ms':30000,
      'topic.metadata.refresh.interval.ms':2000,
      kafkaJS:{acks:ackValue(s.acks),idempotent:s.idempotent,maxInFlightRequests:s.idempotent?5:1,allowAutoTopicCreation:false}});
    await producer.connect();
    return null;
  }
  if(request.op==='send'){
    if(!producer)throw new Error('Kafka producer host is not connected');
    const value=Buffer.from(request.value);
    const message=request.partition===undefined?{key:request.key,value}:{key:request.key,value,partition:request.partition};
    const [ack]=await producer.send({topic:request.topic,messages:[message]});
    const offset=ack?.baseOffset??ack?.offset;
    return {partition:ack?.partition??-1,offset:offset===undefined||offset===null||offset==='-1'?null:String(offset)};
  }
  if(request.op==='admin'){
    if(!admin)throw new Error('Kafka admin host is not connected');
    const a=admin;
    switch(request.method){
      case 'fetchTopicOffsets':return plain(await a.fetchTopicOffsets(request.args[0] as string,{timeout:ADMIN_TIMEOUT_MS} as never));
      case 'fetchOffsets':return plain(await a.fetchOffsets({...(request.args[0] as {groupId:string;topics:string[]}),timeout:ADMIN_TIMEOUT_MS}));
      case 'createTopics':return plain(await a.createTopics(request.args[0] as Parameters<KafkaJS.Admin['createTopics']>[0]));
      case 'deleteTopics':return plain(await a.deleteTopics(request.args[0] as Parameters<KafkaJS.Admin['deleteTopics']>[0]));
    }
  }
  await Promise.allSettled([producer?.disconnect(),admin?.disconnect()]);
  setImmediate(()=>process.exit(0));
  return null;
}

function reply(message:HostReply):void{ process.send?.(message); }

process.on('message',(request:HostRequest)=>{
  handle(request).then(result=>reply({id:request.id,ok:true,result}),
    error=>reply({id:request.id,ok:false,error:error instanceof Error?error.message.slice(0,500):String(error)}));
});
// The engine stopped or crashed: never outlive it.
process.on('disconnect',()=>process.exit(0));
