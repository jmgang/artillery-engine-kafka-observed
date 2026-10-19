/** Producer durability and key-distribution settings used by Kafka-specific load and chaos experiments. */
export type Acks='all'|'1'|'0';
export interface ProducerSettings {
  /** all: wait for min.insync.replicas; 1: leader only (can lose acknowledged writes on leader crash); 0: no acknowledgement. */
  acks:Acks;
  /** Idempotent producer (librdkafka enable.idempotence: no duplicates from retries, order kept); requires acks=all. */
  idempotent:boolean;
  /** Share (0..1) of keys whose messages all go to hotPartition, simulating key skew. */
  hotPartitionShare:number;
  hotPartition:number;
}
export const DEFAULT_PRODUCER_SETTINGS:ProducerSettings={acks:'all',idempotent:true,hotPartitionShare:0,hotPartition:0};

/** Accepts typed values or strings (Artillery `{{ $env.* }}` templates render strings; empty = default). */
export function producerSettings(input:Partial<Record<keyof ProducerSettings,unknown>>={}):ProducerSettings{
  const text=(v:unknown)=>v===undefined||v===null?'':String(v).trim();
  const acks=(text(input.acks)||'all').toLowerCase().replace('-1','all') as Acks;
  if(!['all','1','0'].includes(acks))throw new Error('acks must be all, 1 or 0');
  const idem=text(input.idempotent).toLowerCase();
  if(idem&&idem!=='true'&&idem!=='false')throw new Error('idempotent must be true or false');
  const idempotent=idem?idem==='true':acks==='all';
  if(idempotent&&acks!=='all')throw new Error('An idempotent producer requires acks=all');
  const hotPartitionShare=Number(text(input.hotPartitionShare)||0),hotPartition=Number(text(input.hotPartition)||0);
  if(!(hotPartitionShare>=0&&hotPartitionShare<=1))throw new Error('hotPartitionShare must be between 0 and 1');
  if(!Number.isInteger(hotPartition)||hotPartition<0)throw new Error('hotPartition must be a non-negative integer');
  return {acks,idempotent,hotPartitionShare,hotPartition};
}

/** Reads PRODUCER_ACKS, PRODUCER_IDEMPOTENT, HOT_PARTITION_SHARE and HOT_PARTITION. */
export function producerSettingsFromEnv(env:NodeJS.ProcessEnv=process.env):ProducerSettings{
  return producerSettings({acks:env.PRODUCER_ACKS,idempotent:env.PRODUCER_IDEMPOTENT,hotPartitionShare:env.HOT_PARTITION_SHARE,hotPartition:env.HOT_PARTITION});
}

export function ackValue(acks:Acks):-1|1|0{return acks==='all'?-1:acks==='1'?1:0;}

/** FNV-1a; stable across runs so a key is either always hot or never. */
export function keyShare(key:string):number{
  let hash=0x811c9dc5;
  for(let i=0;i<key.length;i++){hash^=key.charCodeAt(i);hash=Math.imul(hash,0x01000193)>>>0;}
  return (hash%10000)/10000;
}

/**
 * Explicit partition for a "hot" key, else undefined (the producer's murmur2 key partitioner decides).
 * Keys stay unchanged and per-key order is preserved.
 */
export function hotPartitionFor(key:string,settings:ProducerSettings,partitions:number):number|undefined{
  if(settings.hotPartitionShare<=0||keyShare(key)>=settings.hotPartitionShare)return undefined;
  return settings.hotPartition%partitions;
}
