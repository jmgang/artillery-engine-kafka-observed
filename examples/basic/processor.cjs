'use strict';
// A payload factory: the engine calls it once per arrival and sends what it returns.
// key decides the partition; id is your business id, recorded with the Kafka acknowledgement in published.jsonl.
const { threadId } = require('node:worker_threads');
module.exports = {
  order({ ordinal, scheduledAt }) {
    const id = `order-${threadId}-${ordinal}`;
    return { key: id, id, value: JSON.stringify({ id, amount: 100 + (ordinal % 900), createdAt: scheduledAt.toISOString() }) };
  },
};
