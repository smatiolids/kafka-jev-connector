# Connector Configuration

This is the complete public configuration surface for version 1 of `kafka-jev-connector`. Standard Kafka Connect properties such as `name`, `topics`, `tasks.max`, `key.converter`, and `value.converter` retain their platform-defined behavior.

For the initial low-throughput profile, deployment examples set the standard `consumer.override.max.poll.records=20`. A task processes each delivered batch with at most `jev.max.in.flight` concurrent requests and waits for all required Kafka publications before returning. Size `max.poll.records` so worst-case request and retry time remains comfortably below `consumer.override.max.poll.interval.ms`; the connector does not add a second durable queue.

## Required Properties

| Property | Meaning |
| --- | --- |
| `topics` | Explicit comma-separated input topics. Regex subscriptions are not supported. |
| `output.topic` | Pre-created topic that receives Enriched Records. It must not be an input or dead-letter topic. |
| `errors.deadletter.topic` | Pre-created topic that receives permanent record-specific failures. It must not be an input or output topic. |
| `question.set.id` | Immutable, human-readable Question Set identity, such as `fraud-screening/v1`. |
| `jev.api.key` | TypeSafe AI API key. This is a masked password property. |
| `jev.questions` | JSON object in the exact `questions` shape accepted by the TypeSafe AI API. |
| `state.mode` | `FULL_VALUE` or `TEMPLATE`. |
| `state.template` | Required only for `TEMPLATE`; follows the grammar in [Record Contracts](./record-contracts.md#evaluation-state-templates). |

## Optional Properties

| Property | Default | Meaning |
| --- | --- | --- |
| `jev.endpoint` | `https://api.typesafe.ai/v1/systemone` | Evaluation endpoint. Redirects are rejected. |
| `jev.allow.insecure.http` | `false` | Allows HTTP only for explicit local testing. |
| `jev.model` | `jev-latest` | Requested model alias or pinned versioned ID. IDs matching `jev-MAJOR.MINOR.PATCH` (with an optional SemVer suffix) are pinned; every other non-empty reference is classified as an alias. Pin a version in production. |
| `jev.max.in.flight` | `4` | Maximum concurrent Jev requests per task. |
| `jev.connect.timeout.ms` | `5000` | HTTP connection timeout. |
| `jev.request.timeout.ms` | `10000` | Timeout for one Evaluation Attempt. |
| `jev.retry.max.attempts` | `3` | Total attempts, including the initial call. |
| `jev.retry.initial.backoff.ms` | `250` | Initial exponential retry delay before jitter. |
| `jev.retry.max.retry_after.ms` | `30000` | Maximum accepted delay from a Jev `Retry-After` header. |
| `state.raw_bytes.encoding` | `DISABLED` | `DISABLED` or `UTF-8`; controls raw-byte input decoding. |
| `state.max.bytes` | `0` | Optional UTF-8 Evaluation State limit; `0` disables the connector-side limit. |
| `output.key.mode` | `ORIGINAL` | `ORIGINAL` or `EVALUATION_ID` for Enriched Records. Dead-letter keys remain Source ID. |
| `output.headers.mode` | `NONE` | `NONE` or `COPY` for Enriched Record input headers. Dead-letter output never copies input headers; connector-owned `kafka-jev-evaluation-id` and `kafka-jev-resolved-model` headers are unaffected. |
| `behavior.on.null.values` | `IGNORE` | `IGNORE`, `DLQ`, or `FAIL` for tombstones. |
| `errors.transient.exhausted` | `FAIL` | `FAIL` or `DLQ` after transient Jev retries are exhausted. |
| `output.bootstrap.servers` | none | Required for local operation; Confluent Cloud uses its supplied `kafka.endpoint`. |

## Platform-Supplied Kafka Properties

In Confluent Cloud, the connector reads `kafka.endpoint`, `kafka.api.key`, and `kafka.api.secret` supplied through Kafka API-key authentication. The same credentials authorize the managed sink consumer and the plugin-owned output producer. Use an API key owned by a dedicated service account.

For local operation, `output.bootstrap.servers` identifies the Kafka brokers used by the plugin-owned producer. An unauthenticated local broker uses `PLAINTEXT`; secured local testing may supply the same Kafka API-key properties used in Cloud.

The connector does not expose arbitrary producer overrides. Idempotence, `acks=all`, serializers, Cloud security protocol, and retry classification are mandatory behavior.

The connector uses the Kafka Admin API to describe every configured input, output, and dead-letter topic when the Connector Instance starts. Each task independently describes its output and dead-letter topics before constructing its producer, so a task restart cannot bypass the pre-created-topic check. Kafka 4.2 producers do not support `allow.auto.create.topics` (that setting belongs to consumers), so no producer property can disable creation. There is necessarily a race if a topic is deleted after the Admin check but before a producer metadata request. To guarantee that this race cannot recreate a topic, disable broker-side topic auto-creation (or its managed-platform equivalent) and do not grant the connector principal topic-creation privileges.

## Failure Classification

- Locally detected permanent record failures and Jev record-size rejection go to the dead-letter topic.
- Jev `400`, `404`, `422`, `401`, and `403` fail the task as configuration, protocol, or credential errors.
- Jev `408`, `429`, `529`, `5xx`, timeouts, and connection failures use bounded retry. Exhaustion leaves the batch uncommitted and fails the task by default.
- Unknown HTTP statuses fail safely.
- Output or dead-letter publication failure fails the task without committing the affected batch.
