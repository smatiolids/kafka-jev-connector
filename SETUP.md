# Setup

This guide covers local development and deployment of `kafka-jev-connector` to Confluent Cloud. The connector targets Kafka Connect 4.2 and Java 17. Its deployable artifact is a Confluent-compatible plugin ZIP; the Maven build also produces a normal JAR.

The project is independent and is not affiliated with or endorsed by TypeSafe AI or Confluent.

## Security and Data Boundary

Each Connector Instance sends its generated Evaluation State to the external TypeSafe AI service. Review the applicable TypeSafe AI legal and data-processing terms before using production data. Use `TEMPLATE` state mode to allow-list fields when the full Kafka value is not approved for external processing.

Never commit TypeSafe or Kafka API secrets. The connector marks `jev.api.key` as sensitive, does not log Source Record payloads or generated Evaluation State, and excludes credentials from Enriched and Dead-Letter Records.

## Build

Prerequisites:

- JDK 17
- Maven
- Docker with Docker Compose for local integration

Run the complete build:

```bash
mvn verify
```

The build produces:

```text
target/kafka-jev-connector-<version>.jar
target/kafka-jev-connector-<version>-plugin.zip
```

The ZIP is the deployment artifact for both Confluent Cloud and the supplied local Kafka Connect environment. It contains the connector JAR, runtime dependencies, manifest, Apache-2.0 license, dependency notices, documentation, and sample configuration. Kafka Connect, Kafka client, and logging API dependencies are provided by the runtime rather than bundled.

## Local Smoke Test with Fake Jev

The repository's Docker Compose environment runs Kafka Connect 4.2, Kafka, and a deterministic fake Jev endpoint. It does not require a real TypeSafe API key.

Start the environment:

```bash
docker compose up --build -d
```

Create the pre-required topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic jev-input

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic jev-output

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic jev-dlq
```

Create the connector from the supplied example:

```bash
curl --fail --silent --show-error \
  -X POST http://localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  --data @config/local-connector.json
```

Produce a test record:

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic jev-input \
  --property parse.key=true \
  --property key.separator='|' <<'EOF'
customer-42|{"message":"Please help, this is urgent"}
EOF
```

Consume the Enriched Record:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic jev-output \
  --from-beginning \
  --max-messages 1 \
  --property print.key=true
```

Check connector status and logs:

```bash
curl --fail --silent --show-error http://localhost:8083/connectors/jev-local/status
docker compose logs --no-color connect fake-jev
```

Stop the environment when finished:

```bash
docker compose down
```

## Optional Local Test with TypeSafe AI

Set `TYPESAFE_API_KEY` only in your shell or an ignored `.env` file. Use `https://api.typesafe.ai/v1/systemone`, keep `jev.allow.insecure.http=false`, and replace the fake key in a local connector configuration generated from the sample. Do not store the rendered configuration in the repository.

Use a pinned Jev model version for reproducible production behavior. Aliases such as `jev-latest` are supported, and the connector records the resolved version in every successful Enriched Record.

## Confluent Cloud Prerequisites

You need:

- A Confluent Cloud cluster in a region that supports custom connectors
- Permission to upload custom connector plugins and create connector instances
- Confluent CLI authenticated to the target organization and environment
- A dedicated Confluent service account and a Kafka API key owned by it
- A TypeSafe AI API key

List the runtimes available in your environment and verify Kafka Connect 4.2 with Java 17 before deployment:

```bash
confluent connect custom-connector-runtime list
```

Runtime availability changes over time. Do not silently select an older runtime if 4.2 is unavailable; test and release an explicitly supported connector version instead.

## Create Topics and Access

Pre-create all topics. The connector never creates them:

- Every explicit input topic
- The Enriched Record output topic
- The permanent-failure dead-letter topic

Choose partition counts, retention, compaction, and replication intentionally. Input, output, and dead-letter topics must be distinct. Regex input subscriptions are not supported.

Grant the dedicated service-account API key only the access it needs:

- `READ` and `DESCRIBE` on each input topic
- Consumer-group access required by the managed sink connector
- `WRITE` and `DESCRIBE` on the output topic
- `WRITE` and `DESCRIBE` on the dead-letter topic

Do not grant topic-creation or broad cluster-administration access. Confluent Cloud's connector creation flow accepts an existing Kafka API key and secret; use the key owned by this dedicated service account so both the managed sink consumer and plugin-owned producer operate under the same principal.

## Upload the Plugin

Build and verify the plugin ZIP, then upload:

```text
target/kafka-jev-connector-<version>-plugin.zip
```

In Confluent Cloud:

1. Open **Artifacts → Custom connectors**.
2. Choose **Upload Plugin Artifact** and create a plugin.
3. Use `kafka-jev-connector` as the plugin name.
4. Select the target cloud provider and upload the plugin ZIP.
5. Select Kafka Connect runtime `4.2` and Java `17` when creating the Connector Instance.

The plugin manifest points to `https://github.com/smatiolids/kafka-jev-connector` and marks Jev and Kafka secrets as sensitive.

## Configure Cloud Networking

Allow outbound HTTPS to:

```text
api.typesafe.ai:443
```

Use Confluent Cloud's custom-connector networking step or the equivalent `confluent.custom.connection.endpoints` property. Do not allow a wildcard destination when the fixed TypeSafe endpoint is sufficient. The output producer writes to the same Kafka cluster selected for the Connector Instance.

## Configure the Connector Instance

Start from `config/confluent-cloud-connector.json`. Supply secrets through the Cloud console, CLI secret handling, or your infrastructure secret store rather than source control.

The essential configuration is equivalent to:

```json
{
  "name": "production-jev-evaluation",
  "config": {
    "confluent.connector.type": "CUSTOM",
    "confluent.custom.plugin.id": "<uploaded-plugin-id>",
    "confluent.custom.connect.plugin.runtime": "4.2",
    "confluent.custom.connect.java.version": "17",
    "confluent.custom.connection.endpoints": "api.typesafe.ai:443",
    "kafka.api.key": "<dedicated-service-account-api-key>",
    "kafka.api.secret": "<dedicated-service-account-api-secret>",
    "tasks.max": "1",
    "topics": "jev-input",
    "consumer.override.max.poll.records": "20",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "output.topic": "jev-output",
    "errors.deadletter.topic": "jev-dlq",
    "question.set.id": "support-routing/v1",
    "jev.api.key": "<typesafe-api-key>",
    "jev.model": "jev-1.13.0",
    "jev.questions": "{\"department\":{\"type\":\"choice\",\"instructions\":\"Which team should handle this?\",\"criteria\":{\"billing\":\"Payment issues\",\"technical\":\"Technical issues\",\"sales\":\"Sales questions\"}}}",
    "state.mode": "TEMPLATE",
    "state.template": "Customer message: ${value:/message}",
    "output.key.mode": "ORIGINAL",
    "output.headers.mode": "NONE",
    "behavior.on.null.values": "IGNORE",
    "errors.transient.exhausted": "FAIL"
  }
}
```

For Avro, JSON Schema, or Protobuf input, select the corresponding standard converter and configure Confluent Cloud Schema Registry. The connector receives normalized Kafka Connect values and does not implement wire-format decoding itself.

## Verify Cloud Deployment

1. Confirm the Connector Instance reaches `RUNNING`.
2. Produce one non-sensitive test Source Record to an input topic.
3. Confirm one Enriched Record appears in the output topic with the expected Source ID, Evaluation ID, Question Set ID/hash, state hash, resolved model, and unchanged `jev` response.
4. Confirm no record payload or generated Evaluation State appears in the connector application log topic.
5. Test a missing required template reference and confirm one sanitized Dead-Letter Record appears.
6. Test a temporary fake or controlled service failure outside production and confirm transient exhaustion leaves the batch uncommitted rather than draining input to the dead-letter topic.

## Operating Notes

- Delivery is at least once. Consumers requiring unique effects must deduplicate by Evaluation ID or configure `output.key.mode=EVALUATION_ID` and use a compacted output topic.
- Changing Question Set content requires a new immutable `question.set.id`.
- A moving model alias can resolve to a new model and therefore produce a new Evaluation ID; pin a model version for reproducibility.
- Scale `tasks.max`, `consumer.override.max.poll.records`, and `jev.max.in.flight` together while respecting the TypeSafe account-wide request and token limits.
- Permanent record-specific failures go to the dead-letter topic. Authentication, configuration, protocol, Kafka-publication, and exhausted transient failures stop or retry the task without committing the affected batch by default.
- Dead-Letter Records are diagnostic evidence, not connector input. Manual republishing of the original key/value creates a new Source ID and Evaluation ID.

See [Connector Configuration](docs/configuration.md), [Record Contracts](docs/record-contracts.md), and the [domain glossary](CONTEXT.md) for the full contract.
