'use strict';
// When Artillery finishes sending, wait until every kafka-observed engine has seen its consumers catch up
// (or its drainTimeoutMs passes), so the run's end includes the drain and the reports are complete.
const Engine = require('artillery-engine-kafka-observed');
class KafkaDrain {
  cleanup(done) { Engine.finishAll().then(() => done(null), done); }
}
module.exports = { Plugin: KafkaDrain };
