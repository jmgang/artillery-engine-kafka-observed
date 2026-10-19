import { test } from 'node:test';
import assert from 'node:assert/strict';
import { producerSettings, producerSettingsFromEnv, ackValue, keyShare, hotPartitionFor, DEFAULT_PRODUCER_SETTINGS } from './producer-settings.js';

test('defaults are durable: acks=all, idempotent, no skew',()=>{
  assert.deepEqual(producerSettings(),DEFAULT_PRODUCER_SETTINGS);
  assert.deepEqual(producerSettingsFromEnv({}),DEFAULT_PRODUCER_SETTINGS);
  assert.equal(DEFAULT_PRODUCER_SETTINGS.acks,'all');assert.equal(DEFAULT_PRODUCER_SETTINGS.idempotent,true);
  assert.equal(ackValue('all'),-1);assert.equal(ackValue('1'),1);assert.equal(ackValue('0'),0);
});
test('rendered template strings are accepted; empty means default',()=>{
  assert.deepEqual(producerSettings({acks:'',idempotent:'',hotPartitionShare:'',hotPartition:''}),DEFAULT_PRODUCER_SETTINGS);
  assert.deepEqual(producerSettings({acks:'all',idempotent:'false'}),{...DEFAULT_PRODUCER_SETTINGS,idempotent:false});
  assert.throws(()=>producerSettings({idempotent:'yes'}),/true or false/);
});
test('acks=1 defaults to a non-idempotent producer and rejects idempotence',()=>{
  assert.deepEqual(producerSettings({acks:'1'}),{...DEFAULT_PRODUCER_SETTINGS,acks:'1',idempotent:false});
  assert.throws(()=>producerSettings({acks:'1',idempotent:'true'}),/requires acks=all/);
  assert.equal(producerSettings({acks:'-1'}).acks,'all');
  assert.throws(()=>producerSettings({acks:'2'}));
});
test('skew settings are validated',()=>{
  assert.throws(()=>producerSettings({hotPartitionShare:1.5}));
  assert.throws(()=>producerSettings({hotPartition:-1}));
  assert.deepEqual(producerSettingsFromEnv({HOT_PARTITION_SHARE:'0.5',HOT_PARTITION:'3'}),{...DEFAULT_PRODUCER_SETTINGS,hotPartitionShare:0.5,hotPartition:3});
});
test('no skew leaves partitioning to the producer',()=>{
  for(let i=0;i<100;i++)assert.equal(hotPartitionFor(`run-key-${i}`,DEFAULT_PRODUCER_SETTINGS,12),undefined);
});
test('hot share pins a stable fraction of keys to the hot partition',()=>{
  const s=producerSettings({hotPartitionShare:0.5,hotPartition:15});
  const keys=Array.from({length:4000},(_,i)=>`run-key-${i}`);
  const hot=keys.filter(k=>hotPartitionFor(k,s,12)===3);
  assert.ok(hot.length/keys.length>0.45&&hot.length/keys.length<0.55,`hot share ${hot.length/keys.length}`);
  for(const k of keys)assert.equal(hotPartitionFor(k,s,12),keyShare(k)<0.5?3:undefined);
});
