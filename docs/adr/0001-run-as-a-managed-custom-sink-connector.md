---
status: accepted
---

# Run as a managed custom sink connector

Confluent Cloud must execute the integration, so `kafka-jev-connector` will be a Java custom sink connector rather than a self-hosted Kafka Streams application. Although sink connectors normally terminate at an external system, this connector will call Jev and use a plugin-owned producer to publish Enriched Records back to the same Kafka cluster; this deliberate deviation accepts extra producer and credential handling in exchange for managed execution in Confluent Cloud.

## Considered Options

- A Kafka Streams application fits topic-to-topic processing naturally but would require separately managed compute.
- A source connector could return output through Connect's managed producer but would need to create and authenticate its own input consumer.

## Consequences

The Connector Instance requires Kafka API-key credentials that its managed input consumer and plugin-owned output producer can both use. Input, output, and dead-letter topics must be explicitly distinct to prevent feedback loops.
