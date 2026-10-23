# Kafka Jev Connector

[![CI](https://github.com/smatiolids/kafka-jev-connector/actions/workflows/ci.yml/badge.svg)](https://github.com/smatiolids/kafka-jev-connector/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/smatiolids/kafka-jev-connector)](https://github.com/smatiolids/kafka-jev-connector/releases/latest)

Ask questions about your Kafka messages with an AI model, and get the answers back as Kafka messages.

`kafka-jev-connector` is a Kafka Connect sink connector. It reads records from your input
topics, sends each one to [TypeSafe AI's Jev](https://typesafe.ai) service along with a
set of questions you define, and publishes an **Enriched Record** to an output topic. That
record holds the original message, the answers from Jev, and the evidence needed to trace
and deduplicate each evaluation.

Typical uses: classifying support tickets, scoring sentiment or tone, routing messages to
teams, or flagging content, all without writing a consumer service.

[![Watch the Kafka Jev Connector demo](https://img.youtube.com/vi/C1CLjmetbmI/maxresdefault.jpg)](https://youtu.be/C1CLjmetbmI)

**[Watch the Kafka Jev Connector demo on YouTube](https://youtu.be/C1CLjmetbmI)**

| | |
| --- | --- |
| **Runtime** | Kafka Connect 4.2, Java 17 |
| **Target platform** | Confluent Cloud managed custom connectors (also runs on self-managed Connect) |
| **Delivery** | At least once, with deterministic Source IDs and Evaluation IDs for deduplication |
| **Failures** | Bad records go to a dead-letter topic; transient failures retry and are never silently dropped |

## How it works

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant IN as Input topic
    participant C as Jev connector
    participant J as TypeSafe Jev API
    participant OUT as Output topic
    participant DLQ as Dead-letter topic

    P->>IN: JSON message, e.g. text to classify
    IN->>C: Kafka Connect delivers record
    C->>C: Build Evaluation State (FULL_VALUE or TEMPLATE)<br/>and compute Source ID and Evaluation ID
    C->>J: POST state + Question Set + model
    alt Jev answers
        J-->>C: answers, resolved model, token usage
        C->>OUT: Enriched Record (source + input + evaluation + jev)
        C->>IN: Commit offset
    else Permanent record failure (missing template field, 413)
        C->>DLQ: Sanitized Dead-Letter Record
        C->>IN: Commit offset
    else Transient failure (timeouts, 429, 5xx)
        C->>J: Retry with backoff
        Note over C: If retries run out, the task stops and the<br/>offset stays uncommitted, so nothing is lost
    end
```

1. A producer writes a message to an input topic.
2. The connector turns the message into **Evaluation State**: either the whole value
   (`state.mode=FULL_VALUE`) or a text template with only the fields you allow
   (`state.mode=TEMPLATE`).
3. It sends that state to Jev together with your **Question Set** (`jev.questions`).
4. On success it publishes an Enriched Record to the literal or result-selected
   Destination Topic defined by `output.topic`. On a permanent,
   record-specific error it publishes a sanitized record to `errors.deadletter.topic`.
5. The input offset is committed only after the result is written.

## Quick start

You need:

- A Kafka cluster with Kafka Connect 4.2 (Confluent Cloud custom connectors or a
  self-managed worker)
- A TypeSafe AI API key
- The plugin ZIP, `kafka-jev-connector-<version>-plugin.zip`. Download it from the
  [latest release](https://github.com/smatiolids/kafka-jev-connector/releases/latest),
  along with its `.sha256` file, and check it with
  `shasum -a 256 -c kafka-jev-connector-<version>-plugin.zip.sha256`. To build it
  yourself instead, run `mvn package` and pick it up from `target/` (see
  [DEVELOPMENT.md](DEVELOPMENT.md)).

The steps below use Confluent Cloud.

1. **Create the topics.** The connector never creates topics. You need an input topic,
   every possible output topic, and a dead-letter topic, all distinct.
2. **Upload the plugin.** In Confluent Cloud, go to **Artifacts → Custom connectors**
   and upload the plugin ZIP. Select runtime `4.2` and Java `17`.
3. **Allow egress** to `api.typesafe.ai:443`.
4. **Create the connector** with a configuration like this one:

   ```jsonc
   {
     // Connector implementation class
     "connector.class": "io.github.smatiolids.kafkajev.JevSinkConnector",
     // Number of parallel tasks
     "tasks.max": "1",
     // Input topic(s) to evaluate, comma-separated
     "topics": "messages",
     // Read record keys as plain strings
     "key.converter": "org.apache.kafka.connect.storage.StringConverter",
     // Read record values as JSON
     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
     // Values are plain JSON, without an embedded schema
     "value.converter.schemas.enable": "false",
     // Topic for Enriched Records
     "output.topic": "message_jev",
     // Topic for records that fail permanently
     "errors.deadletter.topic": "message_jev_dlq",
     // Kafka API key and secret, used to read input and write output
     "kafka.api.key": "<kafka-api-key>",
     "kafka.api.secret": "<kafka-api-secret>",
     // TypeSafe AI API key
     "jev.api.key": "<typesafe-api-key>",
     // Jev model: an alias like jev-latest, or a pinned version like jev-1.13.0
     "jev.model": "jev-latest",
     // Name and version of the Question Set; change it whenever the questions change
     "question.set.id": "message_classification/v1",
     // The questions to ask Jev, as a JSON string
     "jev.questions": "{\"friendly\":{\"type\":\"score\",\"instructions\":\"How friendly is the message?\",\"criteria\":[\"defined as a score from 0 to 10, where 0 is 'not friendly at all' and 10 is 'extremely friendly'\",\"neither friendly nor unfriendly\",\"somewhat friendly\",\"very friendly\"]}}",
     // Send the whole message value to Jev (TEMPLATE sends only selected fields)
     "state.mode": "FULL_VALUE",
     // Skip tombstone (null-value) records
     "behavior.on.null.values": "IGNORE"
   }
   ```

   The comments are for reading only. Remove them before you submit the configuration,
   because JSON doesn't allow comments.

   Keep secrets out of source control. In production, pin a model version such as
   `jev-1.13.0` instead of `jev-latest`, and consider `TEMPLATE` mode so only
   approved fields leave your cluster.

5. **Send a message** and read the output topic (see the example below).

[SETUP.md](SETUP.md) has the full walkthrough: service-account permissions, networking,
Schema Registry formats, and verification steps. For a self-managed worker, unpack the
ZIP into the worker's `plugin.path` and register the same configuration through the
Connect REST API, replacing `kafka.api.key`/`kafka.api.secret` with
`output.bootstrap.servers` for an unauthenticated cluster.

For a more complete, end-to-end working demo, see
[kafka_jev_demo](https://github.com/smatiolids/kafka_jev_demo).

## Example

With the configuration above, the Question Set asks one question, `friendly`, scored on
four levels.

### Input

A producer sends this value to the `messages` topic with key
`ca93e77c-6f1e-4c79-9c6f-edd93bd8aced`:

```json
{"text": "I';m mad at you"}
```

Any Kafka producer works. With Python and `confluent-kafka`:

```python
import json, uuid
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "<bootstrap-servers>",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "<kafka-api-key>",
    "sasl.password": "<kafka-api-secret>",
})
producer.produce("messages", json.dumps({"text": "I';m mad at you"}), key=str(uuid.uuid4()))
producer.flush(10)
```

### Output

About three seconds later, this Enriched Record appears on `message_jev`:

```json
{
  "source": {
    "id": "sha256:a0090ce3472317882d2882e0e6d255083e37039a13e07dcda2fb25a21580d64a",
    "topic": "messages",
    "partition": 0,
    "offset": 4,
    "timestamp": "2026-10-02T20:01:35.520Z",
    "key": "ca93e77c-6f1e-4c79-9c6f-edd93bd8aced"
  },
  "input": {
    "text": "I';m mad at you"
  },
  "evaluation": {
    "id": "sha256:465c7632733d48b4f28d4aecdf68498bed1100ba5570382c9e2418e5807abfc9",
    "question_set": {
      "id": "message_classification/v1",
      "hash": "sha256:cb7e76d8c75ec91d65a322dfe7e933ec4b98f420ed73f355bcfee3284fea65dc"
    },
    "state_policy": {
      "mode": "FULL_VALUE",
      "hash": "sha256:73610c3a5bebbe3c5cce82d0621f0d4c65b2a73a78a14e5bbdbd311add1915fa"
    },
    "state": {
      "hash": "sha256:5002966feb10ee2313a9c96b8b8235bbe468c2c766723ee107d92b56b0e8937e"
    },
    "model": {
      "requested": "jev-latest",
      "resolved": "jev-1.13.0"
    },
    "attempt_count": 1,
    "duration_ms": 596,
    "completed_at": "2026-10-02T20:01:38.361718801Z"
  },
  "connector": {
    "name": "jev-techzone",
    "plugin": "kafka-jev-connector",
    "version": "0.2"
  },
  "jev": {
    "model": "jev-1.13.0",
    "answers": {
      "friendly": {
        "type": "score",
        "score": 0.08,
        "confidence": 0.92,
        "legend": {
          "0": "defined as a score from 0 to 10, where 0 is 'not friendly at all' and 10 is 'extremely friendly'",
          "1": "neither friendly nor unfriendly",
          "2": "somewhat friendly",
          "3": "very friendly"
        },
        "probabilities": {
          "0": 0.93,
          "1": 0.07,
          "2": 0,
          "3": 0
        }
      }
    },
    "usage": {
      "input_tokens": 354,
      "output_tokens": 17
    }
  }
}
```

### Reading the result

| Section | What it tells you |
| --- | --- |
| `source` | Where the message came from. `source.id` is a stable hash of topic, partition, and offset. |
| `input` | The original message value, unchanged. |
| `evaluation` | How the answer was produced: Question Set and state hashes, requested vs. resolved model, attempts, and latency. `evaluation.id` is the key to deduplicate on. |
| `connector` | Which connector instance and plugin version wrote the record. |
| `jev` | The Jev response, embedded unchanged. |

Here Jev judged the message **not friendly**: level `0` has probability `0.93`, giving an
overall score of `0.08` with confidence `0.92`. The alias `jev-latest` resolved to
`jev-1.13.0`, and the call took 596 ms on the first attempt.

Each Enriched Record also carries two headers, `kafka-jev-evaluation-id` and
`kafka-jev-resolved-model`, so consumers can route or deduplicate without parsing the body.

## Bounded dynamic routing

By default, `output.topic` is a literal topic name and every successful evaluation goes
there. To route records directly from a Jev answer, put exactly one Routing Expression in
`output.topic`. The connector calls this *bounded* routing because it derives every
possible Destination Topic from the configuration and verifies those topics before it
consumes records. Model output can never introduce an arbitrary topic.

The supported forms are:

| Answer | `output.topic` example | Possible result |
| --- | --- | --- |
| Choice | `support-${answers.department.choice}` | `support-billing`, `support-technical` |
| Score Level | `risk-${answers.urgency.level}` | `risk-0`, `risk-1`, `risk-2` |
| Noul Probability | `review-${answers.needs_review.noul >= 0.7; "human", "automatic"}` | `review-human`, `review-automatic` |
| Score Value | `priority-${answers.urgency.score > 1.5; "high", "normal"}` | `priority-high`, `priority-normal` |

Literal text can appear before and after the expression. Numeric conditions support `>`,
`>=`, `<`, and `<=`; the first quoted fragment is used when the condition is true and the
second when it is false.

### Route by Choice

Use a Choice when each configured criterion maps naturally to a topic-name fragment:

```jsonc
{
  "output.topic": "support-${answers.department.choice}",
  "jev.questions": "{\"department\":{\"type\":\"choice\",\"instructions\":\"Which team should handle this?\",\"criteria\":{\"billing\":\"Payment issues\",\"technical\":\"Technical issues\",\"sales\":\"Sales questions\"}}}"
}
```

If Jev returns:

```json
{
  "answers": {
    "department": {
      "type": "choice",
      "choice": "technical"
    }
  }
}
```

the connector publishes the Enriched Record to `support-technical`. Before startup,
create `support-billing`, `support-technical`, and `support-sales`.

### Route by Score Level

A Score Level is the zero-based criterion with the greatest returned probability. If
multiple levels tie, the higher level wins.

```jsonc
{
  "output.topic": "risk-${answers.urgency.level}",
  "jev.questions": "{\"urgency\":{\"type\":\"score\",\"instructions\":\"How urgent is this?\",\"criteria\":[\"low\",\"medium\",\"high\"]}}"
}
```

Probabilities `{"0":0.1,"1":0.2,"2":0.7}` select `risk-2`. Create `risk-0`,
`risk-1`, and `risk-2` before starting the connector.

### Route by Noul Probability threshold

Use a Noul threshold to select one of two literal fragments:

```jsonc
{
  "output.topic": "review-${answers.needs_review.noul >= 0.7; \"human\", \"automatic\"}",
  "jev.questions": "{\"needs_review\":{\"type\":\"noul\",\"instructions\":\"This message requires human review\"}}"
}
```

A returned Noul Probability of `0.82` selects `review-human`; `0.45` selects
`review-automatic`. Noul values are bounded from `0` through `1`, and configuration fails
if the threshold makes either branch impossible.

### Route by Score Value threshold

Use the continuous Score Value when the routing boundary may fall between rubric levels:

```jsonc
{
  "output.topic": "priority-${answers.urgency.score > 1.5; \"high\", \"normal\"}",
  "jev.questions": "{\"urgency\":{\"type\":\"score\",\"instructions\":\"How urgent is this?\",\"criteria\":[\"low\",\"medium\",\"high\"]}}"
}
```

A Score Value of `1.8` selects `priority-high`; `1.5` selects `priority-normal` because
the operator is strictly `>`. Direct interpolation of a Score Value is rejected because
it would create an unbounded set of topics.

### Topic and failure requirements

- Pre-create every possible Destination Topic and grant the connector principal `WRITE`
  and `DESCRIBE` access to each one.
- Destination Topics must be valid Kafka topic names and must differ from every input
  topic and the fixed dead-letter topic.
- A policy may reference only one configured question and one expression.
- A missing answer, wrong answer type, invalid Choice, or out-of-range numeric value is a
  Routing Contract Violation. The task publishes neither success nor dead-letter output
  and leaves the source offset uncommitted so the record can be retried after correction.
- Routing changes only the Kafka destination. It does not change the Enriched Record,
  Evaluation ID, key, headers, timestamp, or partition selection.

See the [configuration reference](docs/configuration.md#destination-policies) for the
complete grammar and validation rules.

## Good to know

- **Duplicates are possible.** Delivery is at least once. If downstream effects must
  happen once, deduplicate by `evaluation.id`.
- **Question Sets are immutable.** When you change `jev.questions`, also change
  `question.set.id` (for example `.../v2`).
- **Routing is bounded.** `output.topic` may remain literal or contain one Choice, Score
  Level, Noul threshold, or Score threshold expression. Every possible destination is
  derived and checked before records are consumed; see the
  [configuration reference](docs/configuration.md#destination-policies).
- **Data leaves your cluster.** Evaluation State is sent to TypeSafe AI. Use `TEMPLATE`
  mode to send only approved fields, and review TypeSafe's data-processing terms first.
- **Secrets stay out of output.** The connector does not log payloads or Evaluation
  State, and it never writes credentials to output or dead-letter records.

## Documentation

- [SETUP.md](SETUP.md): Confluent Cloud deployment in detail
- [RELEASE_NOTES.md](RELEASE_NOTES.md): changes and compatibility notes by release
- [Connector Configuration](docs/configuration.md): every configuration property
- [Record Contracts](docs/record-contracts.md): Enriched and Dead-Letter Record shapes,
  hashing, and templates
- [CONTEXT.md](CONTEXT.md): domain glossary
- [DEVELOPMENT.md](DEVELOPMENT.md): building, testing, and contributing
- [kafka_jev_demo](https://github.com/smatiolids/kafka_jev_demo): a more complete,
  end-to-end working demo

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

This independent project is not affiliated with or endorsed by TypeSafe AI or Confluent.
