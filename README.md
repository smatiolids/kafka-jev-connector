# Kafka Jev Connector

`kafka-jev-connector` is a Java 17 Kafka Connect 4.2 sink connector that sends
connector-owned Evaluation State and Question Sets to TypeSafe AI's Jev service,
then publishes complete inference evidence or sanitized failure evidence back to
Kafka. It is designed for Confluent Cloud managed custom-connector execution and
provides at-least-once delivery with deterministic Source IDs and Evaluation IDs.

Build and test both release artifacts with:

```bash
mvn verify
```

Run the release-level local verification (Docker Compose required) with:

```bash
./scripts/smoke-test.sh
```

It builds the plugin ZIP, starts Kafka and Kafka Connect 4.2 plus a deterministic
fake Jev service, and verifies the Enriched Record, sanitized dead-letter,
uncommitted transient-failure, and log-security paths.

The build writes a normal connector JAR and a self-contained Confluent plugin
ZIP to `target/`. The ZIP bundles connector runtime dependencies while excluding
Kafka Connect, Kafka client, and logging libraries supplied by the worker.

See [SETUP.md](SETUP.md) for local and Confluent Cloud deployment, including
runtime, Java, network, credential, topic, converter, and connector settings.
See [Connector Configuration](docs/configuration.md) for the complete property
surface and [Record Contracts](docs/record-contracts.md) for output guarantees.

This independent project is not affiliated with or endorsed by TypeSafe AI or
Confluent.
