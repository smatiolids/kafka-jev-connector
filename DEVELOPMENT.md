# Development

How to build, test, and package `kafka-jev-connector`. For using the connector, see
[README.md](README.md) and [SETUP.md](SETUP.md).

## Prerequisites

- JDK 17
- Maven
- Docker with Docker Compose (for the smoke test only)

## Build

```bash
mvn package            # compile, run unit tests, build artifacts
mvn package -DskipTests
```

The build writes two artifacts to `target/`:

| Artifact | Purpose |
| --- | --- |
| `kafka-jev-connector-<version>-plugin.zip` | Deployable Confluent plugin. Upload this to Confluent Cloud or unpack it into a Connect worker's `plugin.path`. |
| `kafka-jev-connector-<version>.jar` | Plain connector JAR. |

The ZIP bundles the connector JAR, its runtime dependencies (Jackson, JSON
canonicalization), the plugin manifest, license, notices, documentation, and sample
configuration. Kafka Connect, Kafka client, and logging libraries are left out because
the worker provides them. The layout is defined in
[src/assembly/plugin.xml](src/assembly/plugin.xml) and the Confluent manifest in
[src/plugin/manifest.json](src/plugin/manifest.json).

## Test

There are three levels of tests.

### Unit tests

```bash
mvn test
```

Runs the JUnit 5 tests in `src/test/java` with the Surefire plugin:

- `JevSinkConnectorTest`: configuration validation
- `JevSinkTaskTest`: record processing, retries, dead-lettering, output contracts
- `JevSecurityBoundaryTest`: secrets and payloads stay out of logs and output

### Integration tests

```bash
mvn verify
```

Runs the unit tests, builds the plugin ZIP, then runs the `*IT` tests with Failsafe:

- `PluginPackagingIT`: the ZIP contains the right manifest, JARs, and sample config, and
  does not bundle libraries the worker provides
- `SmokeEnvironmentIT`: the smoke-test environment files are present and consistent

Run `mvn verify` before opening a pull request.

### Smoke test

```bash
./scripts/smoke-test.sh
```

This is the release-level check. It runs the connector end to end in Docker against a
fake Jev service, so you don't need a TypeSafe API key. The script:

1. Builds the plugin ZIP.
2. Starts Kafka 4.2, Kafka Connect 4.2 with the plugin installed, and a deterministic fake
   Jev endpoint ([compose.yaml](compose.yaml)).
3. Creates `jev-input`, `jev-output`, and `jev-dlq` and registers
   [config/local-connector.json](config/local-connector.json).
4. Checks four paths and prints a checkpoint for each:

| Checkpoint | What it proves |
| --- | --- |
| `ENRICHED_RECORD_OK` | A normal message becomes a contract-compliant Enriched Record |
| `SANITIZED_DEAD_LETTER_OK` | A Jev `413` becomes a Dead-Letter Record with no raw response body or secrets |
| `TRANSIENT_UNCOMMITTED_OK` | Repeated `503`s exhaust retries, fail the task, and leave the offset uncommitted |
| `LOG_BOUNDARY_OK` | Connect and fake-Jev logs contain no API key, payload, or Evaluation State |

The environment is torn down automatically. To keep it for inspection:

```bash
KEEP_SMOKE_ENV=1 ./scripts/smoke-test.sh
docker compose logs --no-color connect fake-jev
docker compose down --volumes
```

Kafka Connect's REST API is exposed on port `18083` by default; override it with
`CONNECT_PORT`.

The fake endpoint uses plain HTTP, which requires `jev.allow.insecure.http=true`. Never
set that in production.

### Testing against the real Jev API

Copy `config/local-connector.json` to an ignored location, then set:

- `jev.endpoint` to `https://api.typesafe.ai/v1/systemone`
- `jev.api.key` to your TypeSafe API key
- `jev.allow.insecure.http` to `false`

Keep the API key in your shell or an ignored `.env` file. Never commit the rendered
configuration.

## Project layout

| Path | Contents |
| --- | --- |
| `src/main/java/.../kafkajev/` | Connector source |
| `src/test/java/.../kafkajev/` | Unit and integration tests |
| `src/assembly/`, `src/plugin/` | Plugin ZIP layout and Confluent manifest |
| `config/` | Sample connector configurations (local and Confluent Cloud) |
| `scripts/smoke-test.sh` | End-to-end smoke test |
| `docs/` | Configuration reference, record contracts, ADRs |
| [CONTEXT.md](CONTEXT.md) | Domain glossary (Source Record, Evaluation State, Question Set, ...) |

Main classes:

| Class | Role |
| --- | --- |
| `JevSinkConnector` / `JevConnectorConfig` | Connector entry point and configuration definition |
| `JevSinkTask` | Consumes records, calls Jev, publishes Enriched or Dead-Letter Records |
| `CompiledStateTemplate` | Builds Evaluation State in `FULL_VALUE` or `TEMPLATE` mode |
| `JevClient` | HTTP client with retry, backoff, and `Retry-After` handling |
| `EvaluationEnvelopeFactory` | Builds the output record bodies |
| `DeterministicIds` / `CanonicalJson` | Source ID, Evaluation ID, and hashes (RFC 8785 + SHA-256) |
| `KafkaPublicationFactory` | Producer for output and dead-letter topics |

Design decisions are recorded in [docs/adr/](docs/adr/).

## Changing the version

The version appears in several places. Update all of them together:

- `pom.xml` (`<version>`)
- `src/main/java/.../Version.java`
- `local-kafka/connect.Dockerfile` (plugin ZIP name used by the smoke test)
- `src/test/java/.../SmokeEnvironmentIT.java`

Then run `mvn verify` and `./scripts/smoke-test.sh`.
