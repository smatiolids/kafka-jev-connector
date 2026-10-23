# Release notes

## 0.2

Version 0.2 adds bounded, result-driven routing for successful Enriched Records.

### Added

- `output.topic` can now contain one Routing Expression based on a configured Choice,
  derived Score Level, Noul Probability threshold, or Score Value threshold.
- Connector and task startup enumerate and verify every possible Destination Topic before
  processing begins. Candidate topics must be valid, pre-created, and distinct from input
  and dead-letter topics.
- Score Level routing selects the level with the greatest returned probability and breaks
  ties toward the higher level.
- Noul and Score threshold routing supports `>`, `>=`, `<`, and `<=` with two bounded
  literal result fragments.
- Runtime Routing Contract Violations fail the task without publishing success or
  dead-letter output, leaving the source offset available for retry after correction.
- The local smoke environment now demonstrates Choice routing to `jev-technical`.

### Compatibility

- Existing literal `output.topic` values continue to behave as fixed destinations; no
  migration is required for Connector Instances that do not enable routing.
- Routing does not change Enriched Record JSON, Evaluation IDs, output keys, headers,
  timestamps, partition selection, retry behavior, or the fixed dead-letter topic.
- Routed deployments must pre-create and grant `WRITE` and `DESCRIBE` access to every
  possible Destination Topic derived from their policy.
