import { test } from 'node:test';
import assert from 'node:assert/strict';
import { engineOptions } from './options.js';

const base={brokers:['127.0.0.1:19092'],topic:'t',group:'g',runId:'run-1',partitions:12,reportDirectory:'/tmp/r'};

test('typed options pass through with defaults',()=>{
  const o=engineOptions(base);
  assert.deepEqual(o.brokers,['127.0.0.1:19092']);assert.equal(o.partitions,12);
  assert.equal(o.observationIntervalMs,1000);assert.equal(o.drainTimeoutMs,45000);assert.equal(o.auth,'none');assert.equal(o.region,undefined);
});
test('strings rendered from {{ $env.* }} are accepted',()=>{
  const o=engineOptions({...base,brokers:'b1:9098, b2:9098,b3:9098',partitions:'24',drainTimeoutMs:'300000',auth:'iam',region:'ap-southeast-1'});
  assert.deepEqual(o.brokers,['b1:9098','b2:9098','b3:9098']);assert.equal(o.partitions,24);assert.equal(o.drainTimeoutMs,300000);
  assert.equal(o.auth,'iam');assert.equal(o.region,'ap-southeast-1');
});
test('unset environment variables are rejected by field name',()=>{
  assert.throws(()=>engineOptions({...base,runId:''}),/runId is required/);
  assert.throws(()=>engineOptions({...base,topic:'{{ $env.RUN_ID }}-input'}),/topic is an unresolved template/);
  assert.throws(()=>engineOptions({...base,brokers:''}),/brokers/);
  assert.throws(()=>engineOptions({...base,partitions:'undefined'}),/partitions is an unresolved template/);
  assert.throws(()=>engineOptions({...base,partitions:'0'}),/partitions must be an integer/);
  assert.throws(()=>engineOptions({...base,runId:'bad id'}),/runId must be/);
  assert.throws(()=>engineOptions(undefined),/required/);
});
test('an empty region or auth falls back to defaults',()=>{
  const o=engineOptions({...base,auth:'',region:''});
  assert.equal(o.auth,'none');assert.equal(o.region,undefined);
});
