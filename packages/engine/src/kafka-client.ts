import { fork, type ChildProcess } from 'node:child_process';
import { resolve } from 'node:path';
import type { ConnectionOptions } from './connection.js';
import type { ProducerSettings } from './producer-settings.js';
import type { AdminMethod, HostReply, HostRequest, HostRole } from './kafka-host.js';

type Call = HostRequest extends infer R ? R extends {id:number} ? Omit<R,'id'> : never : never;

/** The Admin reads and writes the engine needs (lag observation, topic set-up). */
export interface KafkaAdmin {
  fetchTopicOffsets(topic:string):Promise<{partition:number;offset:string;high:string;low:string}[]>;
  fetchOffsets(options:{groupId:string;topics:string[]}):Promise<{topic:string;partitions:{partition:number;offset:string}[]}[]>;
  createTopics(options:{timeout?:number;topics:{topic:string;numPartitions:number;replicationFactor:number;configEntries?:{name:string;value:string}[]}[]}):Promise<unknown>;
  deleteTopics(options:{topics:string[];timeout?:number}):Promise<unknown>;
}

/**
 * Runs one Kafka client (producer or Admin) in a helper process (kafka-host.ts) that this engine starts and stops,
 * because the client's native addon cannot run inside Artillery's worker threads. Users of the engine see nothing
 * of it. Requests travel over Node's IPC channel. If the host dies, pending requests fail and the next request
 * starts a fresh host and reconnects it.
 */
export class KafkaHostClient {
  private child:ChildProcess|undefined;
  private connected:Promise<void>|undefined;
  private nextId=0;
  private readonly pending=new Map<number,{resolve:(value:unknown)=>void;reject:(error:Error)=>void}>();
  private closing=false;
  /** Number of times the host had to be restarted after exiting unexpectedly. */
  restarts=0;

  constructor(private readonly role:HostRole,private readonly connection:ConnectionOptions,private readonly settings:ProducerSettings){}

  private spawn():ChildProcess{
    const child=fork(resolve(__dirname,'kafka-host.js'),[],{serialization:'advanced',stdio:['ignore','inherit','inherit','ipc'],execArgv:[]});
    child.on('message',(reply:HostReply)=>{
      const waiter=this.pending.get(reply.id);if(!waiter)return;this.pending.delete(reply.id);
      if(reply.ok)waiter.resolve(reply.result);else waiter.reject(new Error(reply.error));
    });
    child.on('exit',(code,signal)=>{
      if(this.child===child){this.child=undefined;this.connected=undefined;}
      const error=new Error(`Kafka ${this.role} host exited (${signal??code})`);
      for(const waiter of this.pending.values())waiter.reject(error);
      this.pending.clear();
    });
    return child;
  }

  private raw(child:ChildProcess,request:Call):Promise<any>{
    const id=++this.nextId;
    return new Promise((resolvePromise,reject)=>{
      this.pending.set(id,{resolve:resolvePromise,reject});
      child.send({...request,id} as HostRequest,error=>{ if(error){this.pending.delete(id);reject(error);} });
    });
  }

  /** Starts (or restarts) the host and connects its client. */
  connect():Promise<void>{
    if(this.connected)return this.connected;
    if(this.child===undefined&&this.nextId>0)this.restarts++;
    const child=this.spawn();this.child=child;
    this.connected=this.raw(child,{op:'connect',role:this.role,connection:this.connection,settings:this.settings})
      .catch(error=>{ if(this.child===child){child.kill();this.child=undefined;this.connected=undefined;} throw error; });
    return this.connected;
  }

  private async call(request:Call):Promise<any>{
    if(this.closing)throw new Error(`Kafka ${this.role} host is closing`);
    await this.connect();
    return this.raw(this.child!,request);
  }

  /** Resolves with the acknowledged partition and offset (offset null with acks=0). */
  send(topic:string,key:string,value:Buffer,partition?:number):Promise<{partition:number;offset:string|null}>{
    return this.call(partition===undefined?{op:'send',topic,key,value}:{op:'send',topic,key,value,partition});
  }

  admin():KafkaAdmin{
    const method=(name:AdminMethod)=>(...args:unknown[])=>this.call({op:'admin',method:name,args});
    return {fetchTopicOffsets:method('fetchTopicOffsets'),fetchOffsets:method('fetchOffsets'),
      createTopics:method('createTopics'),deleteTopics:method('deleteTopics')} as KafkaAdmin;
  }

  async close():Promise<void>{
    this.closing=true;
    const child=this.child;if(!child)return;
    const gone=new Promise<void>(done=>child.once('exit',()=>done()));
    try{ await this.raw(child,{op:'close'}); }catch{ /* host already exiting */ }
    await Promise.race([gone,new Promise<void>(done=>setTimeout(()=>{child.kill();done();},5000))]);
  }
}
