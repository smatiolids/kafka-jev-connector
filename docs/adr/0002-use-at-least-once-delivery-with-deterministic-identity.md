---
status: accepted
---

# Use at-least-once delivery with source and evaluation identities

A Jev HTTP request, an output Kafka write, and a Kafka Connect offset commit cannot participate in one atomic transaction. The connector therefore provides at-least-once delivery and assigns each Source Record a stable Source ID plus an Evaluation ID derived from the Source ID, Question Set hash, state-policy hash, and effective model version. Results from unchanged evaluation configuration and the same resolved model share an Evaluation ID, while intentional replay under changed questions, state construction, or effective model receives a new one.

## Consequences

The original Source Record key remains the default output key, so automatic deduplication through log compaction is opt-in rather than guaranteed. Consumers that require unique effects must deduplicate using the Evaluation ID or configure it as the output key. Model aliases are supported for convenience, but production Connector Instances should pin a version; when an alias is used, its resolved version participates in Evaluation ID generation so an alias change creates a distinct result.

A Failed Evaluation that used a model alias may not have a resolved model. Its Evaluation ID uses an `unresolved:<alias>` sentinel; a later successful replay receives a version-specific Evaluation ID and remains correlated through the stable Source ID.

The plugin-owned Kafka producer always enables idempotence and `acks=all`, and the connector waits for confirmed publication before permitting input offsets to commit. These settings reduce producer-retry duplicates but do not provide exactly-once evaluation because neither the Jev HTTP call nor the managed sink-consumer offset can join the producer transaction.
