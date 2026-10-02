package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.connect.sink.SinkRecord;

/** Builds the common version-one evaluation envelope for both publication outcomes. */
final class EvaluationEnvelopeFactory {
  private final JevConnectorConfig config;
  private final String questionSetHash;
  private final String statePolicyHash;

  EvaluationEnvelopeFactory(JevConnectorConfig config) {
    this.config = config;
    questionSetHash = DeterministicIds.questionSetHash(config.questions());
    statePolicyHash =
        DeterministicIds.statePolicyHash(
            config.stateMode().name(),
            config.stateTemplateForHash(),
            config.rawBytesEncoding().configValue());
  }

  BuiltEnvelope enriched(
      SinkRecord sourceRecord,
      JsonNode canonicalKey,
      JsonNode canonicalValue,
      String state,
      InferenceResult inferenceResult,
      int attemptCount,
      long durationMillis) {
    String resolvedModel = inferenceResult.resolvedModel();
    CommonEnvelope common =
        common(
            sourceRecord,
            canonicalKey,
            canonicalValue,
            state,
            resolvedModel,
            attemptCount,
            durationMillis);
    common.evaluation().put("completed_at", now());
    common.envelope().set("jev", inferenceResult.json());
    return common.built(resolvedModel);
  }

  BuiltEnvelope deadLetter(
      SinkRecord sourceRecord,
      JsonNode canonicalKey,
      JsonNode canonicalValue,
      PermanentRecordException failure,
      long durationMillis) {
    String failureModel = config.modelReference().failedEvaluationIdentity();
    CommonEnvelope common =
        common(
            sourceRecord,
            canonicalKey,
            canonicalValue,
            failure.state(),
            failureModel,
            failure.attemptCount(),
            durationMillis);
    common.evaluation().put("failed_at", now());
    common
        .envelope()
        .set(
            "error",
            CanonicalJson.object(
                "category", failure.category(),
                "code", failure.code(),
                "message", sanitize(failure.getMessage())));
    return common.built(failureModel);
  }

  private CommonEnvelope common(
      SinkRecord sourceRecord,
      JsonNode canonicalKey,
      JsonNode canonicalValue,
      String state,
      String effectiveModel,
      int attemptCount,
      long durationMillis) {
    String sourceId =
        DeterministicIds.sourceId(
            sourceRecord.topic(), sourceRecord.kafkaPartition(), sourceRecord.kafkaOffset());
    String evaluationId =
        DeterministicIds.evaluationId(
            sourceId, questionSetHash, statePolicyHash, effectiveModel);

    ObjectNode evaluation = CanonicalJson.MAPPER.createObjectNode();
    evaluation.put("id", evaluationId);
    evaluation.set(
        "question_set",
        CanonicalJson.object(
            "id", config.getString(JevConnectorConfig.QUESTION_SET_ID), "hash", questionSetHash));
    evaluation.set(
        "state_policy",
        CanonicalJson.object("mode", config.stateMode().name(), "hash", statePolicyHash));
    if (state != null) {
      evaluation.set(
          "state", CanonicalJson.object("hash", DeterministicIds.evaluationStateHash(state)));
    }
    evaluation.set(
        "model",
        CanonicalJson.object(
            "requested", config.modelReference().value(), "resolved", effectiveModel));
    evaluation.put("attempt_count", attemptCount);
    evaluation.put("duration_ms", durationMillis);

    ObjectNode envelope = CanonicalJson.MAPPER.createObjectNode();
    envelope.set("source", source(sourceRecord, canonicalKey, sourceId));
    envelope.set("input", canonicalValue);
    envelope.set("evaluation", evaluation);
    envelope.set(
        "connector",
        CanonicalJson.object(
            "name", config.getString(JevConnectorConfig.NAME),
            "plugin", "kafka-jev-connector",
            "version", Version.VALUE));
    return new CommonEnvelope(envelope, evaluation, sourceId, evaluationId);
  }

  private static ObjectNode source(
      SinkRecord sourceRecord, JsonNode canonicalKey, String sourceId) {
    ObjectNode source = CanonicalJson.MAPPER.createObjectNode();
    source.put("id", sourceId);
    source.put("topic", sourceRecord.topic());
    source.put("partition", sourceRecord.kafkaPartition());
    source.put("offset", sourceRecord.kafkaOffset());
    if (sourceRecord.timestamp() == null) {
      source.putNull("timestamp");
    } else {
      source.put(
          "timestamp",
          DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(sourceRecord.timestamp())));
    }
    source.set("key", canonicalKey);
    return source;
  }

  private static String now() {
    return DateTimeFormatter.ISO_INSTANT.format(Instant.now());
  }

  private static String sanitize(String message) {
    String sanitized =
        message
            .replaceAll("[\\p{Cntrl}&&[^\\r\\n\\t]]", "?")
            .replaceAll("[\\r\\n\\t]+", " ");
    return sanitized.length() <= 512 ? sanitized : sanitized.substring(0, 512);
  }

  record BuiltEnvelope(
      ObjectNode json, String sourceId, String evaluationId, String effectiveModel) {}

  private record CommonEnvelope(
      ObjectNode envelope, ObjectNode evaluation, String sourceId, String evaluationId) {
    BuiltEnvelope built(String effectiveModel) {
      return new BuiltEnvelope(envelope, sourceId, evaluationId, effectiveModel);
    }
  }
}
