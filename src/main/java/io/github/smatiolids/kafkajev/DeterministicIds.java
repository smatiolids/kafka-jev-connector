package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;

final class DeterministicIds {
  private DeterministicIds() {}

  static String sourceId(String topic, int partition, long offset) {
    return CanonicalJson.hash(
        CanonicalJson.object("topic", topic, "partition", partition, "offset", offset));
  }

  static String evaluationId(
      String sourceId, String questionSetHash, String statePolicyHash, String effectiveModel) {
    return CanonicalJson.hash(
        CanonicalJson.object(
            "source_id", sourceId,
            "question_set_hash", questionSetHash,
            "state_policy_hash", statePolicyHash,
            "effective_model", effectiveModel));
  }

  static String questionSetHash(JsonNode questions) {
    return CanonicalJson.hash(questions);
  }

  static String statePolicyHash(String mode, String template, String rawBytesEncoding) {
    return CanonicalJson.hash(
        CanonicalJson.object(
            "mode", mode, "template", template, "raw_bytes_encoding", rawBytesEncoding));
  }

  static String evaluationStateHash(String state) {
    return CanonicalJson.hashText(state);
  }
}
