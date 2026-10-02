# Record Contracts

`kafka-jev-connector` emits schemaless UTF-8 JSON. Enriched and dead-letter records do not carry an envelope version; their documented shapes are the version 1 compatibility contract.

## Enriched Record

An Enriched Record retains the canonicalized Source Record key and value, evaluation provenance, and the complete Jev response. The `jev` object is embedded unchanged after the connector verifies only that the response is a JSON object with a usable `model` field.

```json
{
  "source": {
    "id": "sha256:...",
    "topic": "orders",
    "partition": 2,
    "offset": 184,
    "timestamp": "2026-10-02T13:45:12.345Z",
    "key": "customer-42"
  },
  "input": {
    "example": "canonicalized original value"
  },
  "evaluation": {
    "id": "sha256:...",
    "question_set": {
      "id": "fraud-screening/v1",
      "hash": "sha256:..."
    },
    "state_policy": {
      "mode": "TEMPLATE",
      "hash": "sha256:..."
    },
    "state": {
      "hash": "sha256:..."
    },
    "model": {
      "requested": "jev-latest",
      "resolved": "jev-1.13.0"
    },
    "attempt_count": 2,
    "duration_ms": 187,
    "completed_at": "2026-10-02T13:45:12.532Z"
  },
  "connector": {
    "name": "production-fraud-screening",
    "plugin": "kafka-jev-connector",
    "version": "1.0.0"
  },
  "jev": {
    "model": "jev-1.13.0",
    "answers": {},
    "usage": {}
  }
}
```

The Kafka record timestamp preserves the source timestamp. The Kafka key defaults to the canonicalized original key; configuring Evaluation ID as the key is opt-in. Connector-owned headers may expose Evaluation ID and resolved model, but input headers are not copied by default.

## Dead-Letter Record

A Dead-Letter Record describes a permanent record-specific Failed Evaluation. It retains the canonicalized Source Record data and configuration provenance, but excludes generated Evaluation State, Question Set contents, input headers, raw Jev error bodies, and credentials.

```json
{
  "source": {
    "id": "sha256:...",
    "topic": "orders",
    "partition": 2,
    "offset": 184,
    "timestamp": "2026-10-02T13:45:12.345Z",
    "key": "customer-42"
  },
  "input": {
    "example": "canonicalized original value"
  },
  "evaluation": {
    "id": "sha256:...",
    "question_set": {
      "id": "fraud-screening/v1",
      "hash": "sha256:..."
    },
    "state_policy": {
      "mode": "TEMPLATE",
      "hash": "sha256:..."
    },
    "model": {
      "requested": "jev-latest",
      "resolved": "unresolved:jev-latest"
    },
    "attempt_count": 0,
    "duration_ms": 1,
    "failed_at": "2026-10-02T13:45:12.346Z"
  },
  "error": {
    "category": "STATE_BUILDING",
    "code": "MISSING_TEMPLATE_VALUE",
    "message": "Required template reference is missing"
  },
  "connector": {
    "name": "production-fraud-screening",
    "plugin": "kafka-jev-connector",
    "version": "1.0.0"
  }
}
```

Dead-letter Kafka keys are always Source ID. Error messages are sanitized and length-bounded. Dead-Letter Records are diagnostic evidence and are not valid Source Records; manual republishing of their original key and value creates a new Source ID and Evaluation ID.

When Evaluation State construction succeeded before a later permanent failure, `evaluation.state.hash` is included using the same shape as an Enriched Record. It is absent when state construction itself failed.

## Canonical JSON Conversion

Kafka Connect values are represented as follows:

- Decimal values become plain decimal strings.
- Dates become `YYYY-MM-DD` strings.
- Times become ISO local-time strings.
- Timestamps become ISO-8601 UTC instants.
- Bytes become `{ "$type": "bytes", "base64": "..." }`, where `base64` uses the standard padded Base64 alphabet.
- Maps with non-string keys become `{ "$type": "map", "entries": [{ "key": ..., "value": ... }] }`. Entries are ordered by the canonical JSON text of the key, then the value, so map iteration order cannot change the result.
- Structs become objects using their schema field names.
- Integers, floating-point values, booleans, strings, lists, and string-keyed maps use their natural JSON forms.

Raw input bytes are decoded as UTF-8 only when explicitly enabled. Missing or null template references use their configured literal default; without a default, they produce a Dead-Letter Record. Empty strings, false, and zero remain present values.

## Deterministic Identities and Hashes

All deterministic hashes use RFC 8785 JSON Canonicalization Scheme followed by SHA-256 over the canonical UTF-8 bytes. They are rendered as lowercase hexadecimal with a `sha256:` prefix.

- Source ID hashes `{ "topic", "partition", "offset" }`.
- Question Set hash hashes the exact configured `questions` JSON value.
- State Policy hash hashes `{ "mode", "template", "raw_bytes_encoding" }`, with absent fields represented explicitly according to the connector configuration contract.
- Evaluation State hash is SHA-256 over the exact UTF-8 state string sent to Jev; it is not JSON-canonicalized again.
- Evaluation ID hashes `{ "source_id", "question_set_hash", "state_policy_hash", "effective_model" }`.

For a successful alias-based evaluation, `effective_model` is the resolved version returned by Jev. For a Failed Evaluation where an alias could not be resolved, it is `unresolved:<alias>`.

## Evaluation State Templates

In `FULL_VALUE` mode, a string becomes its raw content without JSON quotes, a boolean or number becomes its textual form, a struct, map, or list becomes canonical JSON text, and opt-in UTF-8 bytes become decoded text. Null values follow the configured tombstone policy and never become the literal text `null`.

`TEMPLATE` State Policies support these placeholders:

```text
${value}                              whole canonicalized value
${key}                                whole canonicalized key
${value:/customer/name}               RFC 6901 JSON Pointer lookup
${key:/tenant/id:-unknown}            lookup with a literal default
${header:trace-id}                    last header value as UTF-8
${metadata:topic}
${metadata:partition}
${metadata:offset}
${metadata:timestamp}
```

`:-` begins a literal default. Defaults apply to missing and null references but not empty strings, false, or zero. JSON Pointer escaping uses `~0` for `~` and `~1` for `/`. Resolved objects and arrays are inserted as canonical JSON; strings and primitives use their textual values.

Repeated headers use the last value. Non-UTF-8 header values are permanent record failures unless a default exists. `\${` emits literal `${`, `\\` emits literal `\`, and `\}` is allowed inside a default.

Unknown placeholder sources, unknown metadata names, malformed JSON Pointers, and malformed escaping are connector-configuration errors detected at startup.
