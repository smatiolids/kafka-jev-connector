---
status: accepted
---

# Route with a bounded destination policy

A Connector Instance may route each Enriched Record to exactly one topic derived from its Inference Result. The existing `output.topic` property remains the complete public configuration surface: a literal value preserves fixed-topic behavior, while a value containing one routing expression defines a Destination Policy. Keeping routing bounded allows the connector to retain startup topic checks, narrow Kafka ACLs, and disabled topic auto-creation.

## Expression contract

A Destination Policy contains exactly one `${...}` replacement with optional literal text before and after it. The replacement may be one of:

```text
${answers.<question>.choice}
${answers.<question>.level}
${answers.<question>.noul <operator> <number>; "<true-fragment>", "<false-fragment>"}
${answers.<question>.score <operator> <number>; "<true-fragment>", "<false-fragment>"}
```

The numeric operators are `>`, `>=`, `<`, and `<=`. Choice values are inserted directly. A Score Level is the zero-based level with the greatest returned probability; equal greatest probabilities select the higher level. Raw Score values may only participate in comparisons and cannot be inserted directly.

Routable question names match `[A-Za-z_][A-Za-z0-9_-]*`. The grammar deliberately excludes multiple replacements, Choice conditionals, compound conditions, nesting, arithmetic, functions, and arbitrary JSON paths.

## Validation and failure behavior

Connector startup cross-validates the expression against `jev.questions`, rejects unreachable numeric branches, enumerates every possible rendered topic, and verifies that each topic name is valid, already exists, and differs from every input and dead-letter topic. Tasks repeat the destination-existence check before constructing their producer.

At runtime, only the referenced Routing Answer is interpreted; all unrelated answers remain opaque and are preserved unchanged. A missing answer, mismatched answer type, or invalid referenced value is a Routing Contract Violation: the task fails and leaves the source offset uncommitted instead of publishing a Dead-Letter Record.

## Consequences

Routing does not change the Enriched Record schema, deterministic Evaluation ID, output-key and header policies, or Kafka-selected output partition. Each successful evaluation still produces one publication. The dead-letter topic remains fixed. Changing a Destination Policy may therefore publish a replay under the same Evaluation ID to a different topic.

We reject a separate router mode, unrestricted expression evaluation, multiple-answer routing, and direct interpolation of continuous numeric values because they complicate compatibility or make the destination set impossible to validate before processing.
