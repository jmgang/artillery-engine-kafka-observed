import { test } from 'node:test';
import assert from 'node:assert/strict';
import { calculate } from './observer.js';

test('uses exact offsets beyond Number precision',()=>{
  const base=90071992547409930n;
  const r=calculate(new Map([[0,base+7n],[1,10n]]),new Map([[0,base],[1,8n]]),1000);
  assert.equal(r.lagTotal,'9');assert.equal(r.lagMax,'7');assert.equal(r.lagAvg,4.5);
});
test('uninitialized positions have explicit coverage',()=>{
  const r=calculate(new Map([[0,10n],[1,5n]]),new Map([[0,-1n]]),1000);
  assert.equal(r.partitionsObserved,0);assert.equal(r.partitionsUninitialized,2);assert.equal(r.lagAvg,null);
});
test('offset rollback invalidates rate calculation',()=>{
  const r=calculate(new Map([[0,8n]]),new Map([[0,4n]]),2000,
    {time:1000,ends:new Map([[0,10n]]),commits:new Map([[0,3n]])});
  assert.equal(r.productionRate,null);assert.equal(r.committedRate,null);
});
test('production rate remains available when empty partitions are uninitialized',()=>{
  const r=calculate(new Map([[0,20n],[1,0n]]),new Map([[0,16n],[1,-1n]]),2000,
    {time:1000,ends:new Map([[0,10n],[1,0n]]),commits:new Map([[0,9n],[1,-1n]])});
  assert.equal(r.productionRate,10);assert.equal(r.committedRate,7);
});
test('explicit earliest policy includes backlog of an uninitialized group',()=>{
  const r=calculate(new Map([[0,10n],[1,0n]]),new Map([[0,-1n],[1,-1n]]),1000,undefined,new Map([[0,2n],[1,0n]]));
  assert.equal(r.partitionsObserved,2);assert.equal(r.partitionsUninitialized,2);assert.equal(r.lagTotal,'8');
});
