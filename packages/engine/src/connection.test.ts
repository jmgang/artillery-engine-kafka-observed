import { test } from 'node:test';
import assert from 'node:assert/strict';
import { kafkaConfig, parseAuthMode } from './connection.js';

test('local default is plaintext without SASL',()=>{
  const c=kafkaConfig({brokers:['127.0.0.1:19092'],clientId:'t'}).kafkaJS!;
  assert.deepEqual(c.brokers,['127.0.0.1:19092']);assert.equal(c.ssl,undefined);assert.equal(c.sasl,undefined);
});
test('tls enables TLS only',()=>{
  const c=kafkaConfig({brokers:['b:9094'],clientId:'t',auth:'tls'}).kafkaJS!;
  assert.equal(c.ssl,true);assert.equal(c.sasl,undefined);
});
test('iam uses OAUTHBEARER with a signed MSK token, principal and lifetime',async()=>{
  let seenRegion='';
  const c=kafkaConfig({brokers:['b:9098'],clientId:'t',auth:'iam',region:'ap-southeast-1'},
    async region=>{seenRegion=region;return {token:'signed',expiryTime:1700000000000};}).kafkaJS!;
  assert.equal(c.ssl,true);assert.equal(c.sasl?.mechanism,'oauthbearer');
  const provider=(c.sasl as {oauthBearerProvider:()=>Promise<{value:string;principal:string;lifetime:number}>}).oauthBearerProvider;
  assert.deepEqual(await provider(),{value:'signed',principal:'msk-iam',lifetime:1700000000000});assert.equal(seenRegion,'ap-southeast-1');
});
test('iam without a region is rejected',()=>{
  const saved={a:process.env.AWS_REGION,b:process.env.AWS_DEFAULT_REGION};delete process.env.AWS_REGION;delete process.env.AWS_DEFAULT_REGION;
  try{assert.throws(()=>kafkaConfig({brokers:['b:9098'],clientId:'t',auth:'iam'}),/region/);}
  finally{if(saved.a!==undefined)process.env.AWS_REGION=saved.a;if(saved.b!==undefined)process.env.AWS_DEFAULT_REGION=saved.b;}
});
test('auth mode parsing: empty (unset template) means none',()=>{
  assert.equal(parseAuthMode(undefined),'none');assert.equal(parseAuthMode(''),'none');assert.equal(parseAuthMode('  '),'none');
  assert.equal(parseAuthMode('IAM'),'iam');assert.throws(()=>parseAuthMode('scram'));
});
