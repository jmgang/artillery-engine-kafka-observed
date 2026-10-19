# artillery-engine-kafka-observed

An [Artillery](https://www.artillery.io/) engine for load testing **Kafka consumer applications**.

Artillery generates the traffic you describe (phases such as "20 per second for a minute, then ramp to 135 per second"). This engine sends that traffic to a Kafka topic and records what you need to judge the consumers on the other side:

- **Confirmed delivery.** Each message is produced through the [Confluent Kafka client](https://github.com/confluentinc/confluent-kafka-javascript) (librdkafka), waiting for Kafka to acknowledge it (`acks=all`, idempotent by default).
- **A record of every acknowledged message.** Its id, key, partition and offset go to `published.jsonl`; failed or unknown deliveries go to `publish-failures.jsonl`. Afterwards you can check that every acknowledged message was actually handled.
- **Consumer lag while the test runs.** Every second (configurable) it reads each partition's end offset and your consumer group's committed offset: lag per partition, total and maximum, production and commit rates.
- **A drain at the end.** With the companion plugin, the run stays open until the consumers have caught up (or a timeout passes), so the report includes the catch-up.
- **Amazon MSK IAM authentication** (SASL/OAUTHBEARER over TLS) using the default AWS credential chain, plus plain and TLS connections.

It's useful when the question is "**can my consumers keep up, how far behind do they fall, and did every message get through?**", for example in load tests and chaos tests of a consumer group.

## Why a custom engine

Artillery has built-in engines for HTTP, WebSockets and Socket.IO, but no maintained Kafka engine. Earlier community Kafka plugins only fire messages: they don't wait for Kafka to confirm, don't record what was acknowledged, and don't look at the consumers. This engine was built for load and chaos testing a payment-notification consumer on Amazon MSK, where those three things were the point of the test.

## Packages in this repository

| Path | Package | What it does |
|---|---|---|
| `packages/engine` | `artillery-engine-kafka-observed` | The engine (`engines: kafka-observed`) |
| `packages/drain-plugin` | `artillery-plugin-kafka-drain` | Keeps the run open until the observed consumers catch up (`plugins: kafka-drain`) |
| `examples/basic` | | A runnable example against a local Kafka |

## Quick start (local Kafka)

Requirements: Node.js 24 and Docker. `npm install` also installs Artillery 2.0.34 for the example; Artillery loads engines from next to its own installation, so install it in the same project as the engine.

```sh
git clone https://github.com/jmgang/artillery-engine-kafka-observed.git
cd artillery-engine-kafka-observed
npm install
npm run patch-artillery      # see "The Artillery patch" below
npm run build
npm test

# a single local Kafka broker, and a topic with 3 partitions
docker compose -f examples/basic/compose.yaml up -d
docker compose -f examples/basic/compose.yaml exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic example-orders --partitions 3

# a consumer in the observed group (stands in for your application)
docker compose -f examples/basic/compose.yaml exec -d kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic example-orders --group example-group

# the load test
KAFKA_BOOTSTRAP=localhost:9092 RUN_ID=demo-1 ARTILLERY_LOCAL_WORKERS=1 npx artillery run examples/basic/test.yml
```

The results land in `examples/basic/out/demo-1/`. Stop the consumer and the run will wait for the lag to drain until `drainTimeoutMs`, then fail. That's the point: the drain gate tells you the consumers didn't catch up.

## The Artillery patch

Stock Artillery 2.0.34 doesn't wait for its worker threads to finish when a run stops, which cuts the drain short (no `engine-report.json`, lag still above zero). `patches/artillery-2.0.34.patch` changes two lines in Artillery's local runner:

1. On stop, wait (up to 60 s) for each worker to exit, so the drain can finish.
2. Read `ARTILLERY_LOCAL_WORKERS` for the number of workers. With `1`, there's one producer and one lag observer, which keeps the observation clean.

Apply it after `npm install` with `npm run patch-artillery` (safe to run twice). Without it, sending and recording still work, but the drain and the end-of-run report don't.

## Using it in your own test

```yaml
config:
  target: kafka://cluster                  # required by Artillery; not used by the engine
  processor: ./processor.cjs               # your payload factory
  engines:
    kafka-observed: {}
  plugins:
    kafka-drain: {}
  phases:
    - { name: baseline, duration: 120, arrivalRate: 45 }
    - { name: surge, duration: 180, arrivalRate: 135 }
  kafkaObserved:
    brokers: "{{ $env.KAFKA_BOOTSTRAP }}"
    topic: orders
    group: order-service                   # the consumer group to observe
    partitions: 12                         # must equal the topic's partition count
    runId: "{{ $env.RUN_ID }}"
    reportDirectory: "{{ $dirname }}/out/{{ $env.RUN_ID }}"
scenarios:
  - engine: kafka-observed
    flow:
      - publish:
          factory: order                   # a function exported by the processor
```

The **payload factory** is a synchronous function in your processor file. The engine calls it once per arrival with `{ ordinal, scheduledAt, vars }`; it returns `{ key, value, id? }`:
- `key` decides the partition;
- `value` is a string or Buffer;
- `id` is your business id, recorded with the acknowledgement.

```js
module.exports = {
  order({ ordinal, scheduledAt }) {
    const id = `order-${ordinal}`;
    return { key: id, id, value: JSON.stringify({ id, createdAt: scheduledAt.toISOString() }) };
  },
};
```

If Artillery runs several workers, make ids unique per worker (the example adds the worker's thread id).

### Options (`config.kafkaObserved`)

Values can be literal or `{{ $env.NAME }}` templates. An unset variable is rejected with the field's name, so a run never starts half-configured.

| Option | Required | Default | Meaning |
|---|---|---|---|
| `brokers` | yes | | Bootstrap brokers, a list or a comma-separated string |
| `topic` | yes | | Topic to produce to |
| `group` | yes | | Consumer group whose lag is observed |
| `partitions` | yes | | The topic's partition count; the drain only passes when every partition is observed |
| `runId` | yes | | 1–50 characters `[a-zA-Z0-9-]`; used in the client id |
| `reportDirectory` | yes | | Where the output files go |
| `observationIntervalMs` | | 1000 | How often lag is sampled (minimum 100) |
| `drainTimeoutMs` | | 45000 | How long the drain waits for lag to reach zero; 0 skips waiting (and fails the gate) |
| `auth` | | `none` | `none` (plaintext), `tls`, or `iam` (Amazon MSK IAM) |
| `region` | for `iam` | `AWS_REGION` | AWS region for MSK IAM tokens |
| `acks` | | `all` | `all`, `1` or `0` |
| `idempotent` | | `true` with `acks=all` | Idempotent producer (requires `acks=all`) |
| `hotPartitionShare`, `hotPartition` | | 0, 0 | Send this share (0–1) of keys to one partition, to simulate key skew; keys and per-key order are kept |

An optional `config.experiment` block (any object, for example a hypothesis and pass/fail checks) is saved with the phases as `experiment.json`, for your own verification step.

### Output files (in `reportDirectory`)

| File | Contents |
|---|---|
| `published.jsonl` | One line per acknowledged message: `id`, `key`, `partition`, `offset`, `ackedAt` |
| `publish-failures.jsonl` | One line per failed send: delivery unknown, since the broker may or may not have it |
| `engine-samples.jsonl` | One line per sample: total, max and average lag; per-partition end and committed offsets; production and commit rates; the Artillery phase; `drain` after sending stops |
| `engine-report.json` | Totals: sent, failed, delivery unknown, send latency, producer settings, whether the drain passed |
| `experiment.json` | Your `config.experiment`, if present |

The engine also emits Artillery metrics, so they appear in Artillery's own report:
- `kafka.messages_sent`, `kafka.messages_failed`, `kafka.messages_delivery_unknown`, `kafka.send_latency_ms`;
- `kafka.lag.total` / `.max` / `.avg`, `kafka.production_rate`, `kafka.committed_rate`;
- `load.requested_rate`, `load.actual_rate`, `load.schedule_delay_ms`;
- the generator's own CPU, memory and event-loop delay.

## Amazon MSK with IAM

```yaml
  kafkaObserved:
    brokers: "{{ $env.KAFKA_BOOTSTRAP }}"   # the IAM bootstrap string (port 9098)
    auth: iam
    region: "{{ $env.AWS_REGION }}"
```

Credentials come from the default AWS chain (ECS task role, instance profile, environment, profile). The role needs, on the cluster, topic and group:
- `kafka-cluster:Connect`, `DescribeCluster`;
- `WriteData`, `DescribeTopic`;
- `DescribeGroup` (to read the group's committed offsets).

## How it works

- **One producer and one admin client per Artillery worker**, never one per virtual user.
- **Each client runs in its own helper process.** Artillery runs engines in worker threads, and the librdkafka native addon can't run inside a worker thread. The engine starts the helpers itself and talks to them over IPC; a crashed helper is restarted, and the restarts are counted in the report.
- **Arrival rate is set by Artillery**, independent of how fast Kafka acknowledges. Scheduling delay is reported, so you can see if the generator itself fell behind.
- **Lag is measured from committed offsets**, the consumers' saved progress. Committed progress is not the same as completed work: verify your application's own outcome against `published.jsonl` for that.
- **Before a group has committed**, a partition's lag counts from the earliest offset (`partitionsUninitialized` in the samples).

## Limitations

- One topic and one consumer group per engine.
- Lag is offset-based. It tells you how far behind the consumers are, not whether they handled each message correctly.
- Tested with Artillery 2.0.34, Node.js 24 and Kafka 4.2 (Amazon MSK Express and Apache Kafka).

## License

[MIT](LICENSE)
