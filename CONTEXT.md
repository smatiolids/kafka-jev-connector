# Jev Inference Connector

This context describes how Kafka records become Jev evaluations and enriched Kafka records. It separates the source data, evaluation configuration, and returned inference evidence.

## Language

**Source Record**:
A Kafka record selected for evaluation, including its key, value, headers, timestamp, topic, partition, and offset.
_Avoid_: Message, event

**Question Set**:
An immutable, versioned collection of typed Jev questions evaluated together against one Evaluation State. Its identifier must change whenever its question types, instructions, criteria, or membership changes; its shape is the `questions` map accepted by the TypeSafe AI API.
_Avoid_: Prompt, rules, decision logic

**Evaluation State**:
The text derived from a Source Record and presented to Jev alongside a Question Set.
_Avoid_: Prompt, payload

**State Policy**:
The immutable rule for deriving Evaluation State from a Source Record, either by full-value stringification or configured placeholder interpolation. A placeholder may declare a literal default used for a missing or null reference; either condition without a default produces a Failed Evaluation, while empty strings, false, and zero remain present values. The policy's canonical hash contributes to Evaluation ID.
_Avoid_: Template, mapping

**Jev Evaluation**:
One logical assessment of an Evaluation State against a Question Set, producing an Inference Result after one or more Evaluation Attempts.
_Avoid_: Processing, decision call

**Evaluation Attempt**:
One HTTP call to Jev within a Jev Evaluation. An Enriched Record retains only the total attempt count and aggregate duration, while a Dead-Letter Record retains the attempt count and sanitized final error.
_Avoid_: Evaluation, retry

**Inference Result**:
The complete, valid JSON response returned by Jev for an evaluation. The connector reads the resolved model for identity and provenance but otherwise treats the response as opaque and preserves it unchanged.
_Avoid_: Decision, prediction

**Enriched Record**:
The output Kafka record containing source identity, the original key and value, aggregate evaluation provenance, and the complete Inference Result.
_Avoid_: Result message, processed event

**Routing**:
The assignment of an Enriched Record to exactly one Destination Topic based on its Inference Result.
_Avoid_: Fan-out, broadcasting

**Routing Answer**:
The typed answer from one configured question whose fields are evaluated to select a Destination Topic. A Choice contributes its selected choice, a Noul contributes its probability, and a Score contributes either its numeric value or its derived Score Level.
_Avoid_: Classification, routing decision

**Noul Probability**:
A numeric probability expressing how likely the statement in a Noul question is to be true.
_Avoid_: Boolean, confidence

**Score Value**:
The numeric rating returned for a Score question against its ordered criteria.
_Avoid_: Noul Probability, confidence

**Score Level**:
The zero-based level with the greatest probability in a Score answer. When levels tie for greatest probability, the higher level is selected.
_Avoid_: Score Value, criteria description

**Destination Topic**:
The single Kafka topic selected to receive an Enriched Record.
_Avoid_: Output channel, route

**Destination Policy**:
The immutable rule that derives one Destination Topic from an Inference Result. It combines literal text with exactly one Routing Expression, and every possible destination must be valid before evaluation begins.
_Avoid_: Router mode, dynamic topic

**Routing Expression**:
The single replacement in a Destination Policy. It yields a selected Choice, a Score Level, or one fragment selected by a numeric Noul or Score comparison.
_Avoid_: Placeholder, script

**Routing Contract Violation**:
An Inference Result that cannot satisfy the Destination Policy because a referenced answer is absent, has an unexpected type, or contains an invalid referenced value. It stops processing rather than becoming a Dead-Letter Record because it violates the connector's configured evaluation contract.
_Avoid_: Failed Evaluation, unroutable record

**Source ID**:
A deterministic identity derived from a Source Record's topic, partition, and offset. It remains stable whenever that Source Record is replayed.
_Avoid_: Message ID, event ID

**Evaluation ID**:
A deterministic identity derived from the Source ID, Question Set hash, State Policy hash, and effective model version. The effective version is the resolved model when available; a Failed Evaluation whose alias cannot be resolved uses an `unresolved:<alias>` sentinel.
_Avoid_: Request ID, correlation ID, Inference ID

**Failed Evaluation**:
A Jev Evaluation for which no valid Inference Result was produced. A permanent record-specific failure becomes a Dead-Letter Record, while exhausted transient service failures leave the Kafka batch uncommitted and trigger task recovery by default.
_Avoid_: Bad message, poison pill

**Dead-Letter Record**:
A sanitized record describing a permanent record-specific Failed Evaluation while retaining enough source identity and error context for diagnosis or replay.
_Avoid_: Error message

**Connector Instance**:
A configured deployment of the plugin with a fixed Question Set, state-building policy, Destination Policy, input topics, and dead-letter topic.
_Avoid_: Connector, worker
