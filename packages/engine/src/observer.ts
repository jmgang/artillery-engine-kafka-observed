import type { KafkaAdmin } from './kafka-client.js';
import { performance } from 'node:perf_hooks';

export interface Sample {
  timestamp: string; lagTotal: string; lagMax: string; lagAvg: number | null;
  partitionsObserved: number; partitionsUninitialized: number; committedRate: number | null;
  productionRate: number | null; ageSeconds: number; errors: number;
  positions?: {partition:number;endOffset:string;committedOffset:string|null;initialPosition:string|null}[];
}
interface Positions { time: number; ends: Map<number,bigint>; commits: Map<number,bigint>; }
const safeNumber=(value: bigint): number|null => value>BigInt(Number.MAX_SAFE_INTEGER)||value<BigInt(Number.MIN_SAFE_INTEGER)?null:Number(value);
export function calculate(ends: Map<number,bigint>, commits: Map<number,bigint>, now: number, previous?: Positions, earliest?: Map<number,bigint>): Sample {
  let total=0n,max=0n,observed=0,uninitialized=0;
  for(const [p,end] of ends) {
    let committed=commits.get(p);
    if(committed===undefined||committed<0n) {uninitialized++;committed=earliest?.get(p);if(committed===undefined)continue;}
    // Negative lag invalidates coverage instead of being turned into a reassuring zero.
    if(end<committed) continue;
    const lag=end-committed; total+=lag;if(lag>max)max=lag;observed++;
  }
  let productionRate:number|null=null,committedRate:number|null=null;
  if(previous&&now>previous.time) {
    const elapsed=(now-previous.time)/1000;
    const endChange=sameKeys(ends,previous.ends)?delta(ends,previous.ends):null;
    if(endChange!==null)productionRate=endChange/elapsed;
    if(endChange!==null&&sameKeys(commits,previous.commits)) {
      const change=delta(commits,previous.commits,true);if(change!==null)committedRate=change/elapsed;
    }
  }
  const totalNumber=safeNumber(total);
  return {timestamp:new Date().toISOString(),lagTotal:total.toString(),lagMax:max.toString(),
    lagAvg:observed&&totalNumber!==null?totalNumber/observed:null,partitionsObserved:observed,
    partitionsUninitialized:uninitialized,committedRate,productionRate,ageSeconds:0,errors:0,
    positions:[...ends].map(([partition,end])=>({partition,endOffset:end.toString(),
      committedOffset:commits.has(partition)?commits.get(partition)!.toString():null,
      initialPosition:earliest?.get(partition)?.toString()??null}))};
}
function sameKeys(a:Map<number,bigint>,b:Map<number,bigint>):boolean{return a.size===b.size&&[...a.keys()].every(p=>b.has(p));}
function delta(a:Map<number,bigint>,b:Map<number,bigint>,committed=false):number|null{
  let total=0n;for(const [p,value] of a){const old=b.get(p)!;
    if(committed&&value===-1n&&old===-1n)continue;
    if(value<0n||old<0n||value<old)return null;total+=value-old;}
  return safeNumber(total);
}
/** One observer per topic/group. No per-virtual-user Admin connection. */
export class Observer {
  private previous?:Positions; private lastSuccess?:number; private errors=0;
  constructor(private admin:KafkaAdmin,private topic:string,private group:string,private startPolicy:'unknown'|'earliest'='unknown'){}
  async sample():Promise<Sample>{
    try{
      // End offsets + committed offsets only. (describeGroups crashed the native client while a broker was down;
      // consumer assignment is recorded by the consumers' own rebalance records.)
      const [endRows,groupRows]=await Promise.all([this.admin.fetchTopicOffsets(this.topic),
        this.admin.fetchOffsets({groupId:this.group,topics:[this.topic]})]);
      const now=performance.now();
      const ends=new Map(endRows.map(r=>[r.partition,BigInt(r.high)]));
      const commits=new Map((groupRows.find(r=>r.topic===this.topic)?.partitions??[]).map(r=>[r.partition,BigInt(r.offset)]));
      const earliest=this.startPolicy==='earliest'?new Map(endRows.map(r=>[r.partition,BigInt(r.low)])):undefined;
      const sample=calculate(ends,commits,now,this.previous,earliest);sample.errors=this.errors;
      this.previous={time:now,ends,commits};this.lastSuccess=now;return sample;
    }catch(error){this.errors++;throw error;}
  }
  status():{ageSeconds:number|null;errors:number}{return {ageSeconds:this.lastSuccess===undefined?null:(performance.now()-this.lastSuccess)/1000,errors:this.errors};}
}
