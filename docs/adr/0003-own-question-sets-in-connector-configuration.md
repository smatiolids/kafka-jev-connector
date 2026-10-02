---
status: accepted
---

# Own question sets in connector configuration

Each Connector Instance owns a fixed Question Set in the same JSON shape accepted by the TypeSafe AI API. Source Records supply data but cannot supply or alter questions, preventing untrusted records from changing evaluation behavior or API cost; changing a Question Set's type, instructions, criteria, or membership requires connector reconfiguration and a new immutable question-set identifier.

## Consequences

Enriched Records contain the Question Set's identifier and canonical hash rather than repeating the questions. They contain every returned answer, with no connector-defined generic `decision` field; a question ID may represent a business decision when a use case needs one.
