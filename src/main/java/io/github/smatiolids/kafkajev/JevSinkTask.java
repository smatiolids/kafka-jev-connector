package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public final class JevSinkTask extends SinkTask {
  private static final HttpHeaders EMPTY_HEADERS = HttpHeaders.of(Map.of(), (name, value) -> true);

  private final Function<Map<String, Object>, Producer<String, String>> producerFactory;
  private final Sleeper sleeper;
  private final DoubleSupplier jitterMultiplier;
  private final LongSupplier monotonicNanos;
  private final ConnectValueCanonicalizer canonicalizer = new ConnectValueCanonicalizer();
  private JevConnectorConfig config;
  private Producer<String, String> producer;
  private HttpClient http;
  private volatile boolean batchFailed;

  public JevSinkTask() {
    this(properties -> new KafkaProducer<>(properties));
  }

  JevSinkTask(Function<Map<String, Object>, Producer<String, String>> producerFactory) {
    this(
        producerFactory,
        Thread::sleep,
        () -> 0.5 + ThreadLocalRandom.current().nextDouble(),
        System::nanoTime);
  }

  JevSinkTask(
      Function<Map<String, Object>, Producer<String, String>> producerFactory,
      Sleeper sleeper,
      DoubleSupplier jitterMultiplier,
      LongSupplier monotonicNanos) {
    this.producerFactory = producerFactory;
    this.sleeper = sleeper;
    this.jitterMultiplier = jitterMultiplier;
    this.monotonicNanos = monotonicNanos;
  }

  @Override
  public void start(Map<String, String> properties) {
    config = new JevConnectorConfig(properties);
    producer = producerFactory.apply(producerProperties(config));
    http =
        HttpClient.newBuilder()
            .connectTimeout(
                Duration.ofMillis(config.getInt(JevConnectorConfig.CONNECT_TIMEOUT_MS)))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    batchFailed = false;
    try {
      for (SinkRecord sourceRecord : records) {
        evaluateAndPublish(sourceRecord);
      }
    } catch (RuntimeException failure) {
      batchFailed = true;
      throw failure;
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return batchFailed ? Map.of() : super.preCommit(currentOffsets);
  }

  @Override
  public void stop() {
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public String version() {
    return Version.VALUE;
  }

  private void evaluateAndPublish(SinkRecord sourceRecord) {
    if (sourceRecord.value() == null
        && "IGNORE".equals(config.getString(JevConnectorConfig.TOMBSTONE_BEHAVIOR))) {
      return;
    }
    if (sourceRecord.value() == null
        && "FAIL".equals(config.getString(JevConnectorConfig.TOMBSTONE_BEHAVIOR))) {
      throw new ConnectException("Tombstone Source Record configured to fail the task");
    }
    long startedAt = monotonicNanos.getAsLong();
    JsonNode canonicalValue =
        canonicalizer.canonicalize(sourceRecord.valueSchema(), sourceRecord.value());
    JsonNode canonicalKey = canonicalizer.canonicalize(sourceRecord.keySchema(), sourceRecord.key());
    if (sourceRecord.value() == null) {
      publishDeadLetter(
          sourceRecord,
          canonicalKey,
          canonicalValue,
          new PermanentRecordException(
              "STATE_BUILDING", "TOMBSTONE", "Tombstone Source Record configured for dead letter"),
          elapsedMillis(startedAt));
      return;
    }

    String state;
    try {
      state = evaluationState(sourceRecord, canonicalKey, canonicalValue);
      long maximumBytes = config.getLong(JevConnectorConfig.STATE_MAX_BYTES);
      if (maximumBytes > 0 && state.getBytes(StandardCharsets.UTF_8).length > maximumBytes) {
        throw new PermanentRecordException(
            "STATE_BUILDING",
            "STATE_TOO_LARGE",
            "Evaluation State exceeds the configured byte limit",
            state);
      }
    } catch (TemplateResolutionException failure) {
      String code =
          "Template header is not valid UTF-8".equals(failure.getMessage())
              ? "INVALID_UTF8"
              : "MISSING_TEMPLATE_VALUE";
      publishDeadLetter(
          sourceRecord,
          canonicalKey,
          canonicalValue,
          new PermanentRecordException("STATE_BUILDING", code, failure.getMessage()),
          elapsedMillis(startedAt));
      return;
    } catch (PermanentRecordException failure) {
      publishDeadLetter(
          sourceRecord, canonicalKey, canonicalValue, failure, elapsedMillis(startedAt));
      return;
    }

    EvaluationSuccess success;
    try {
      success = callJev(state);
    } catch (PermanentRecordException failure) {
      publishDeadLetter(
          sourceRecord, canonicalKey, canonicalValue, failure, elapsedMillis(startedAt));
      return;
    }
    long durationMillis = elapsedMillis(startedAt);
    ObjectNode enriched =
        enrichedRecord(
            sourceRecord,
            canonicalKey,
            canonicalValue,
            state,
            success.inferenceResult(),
            success.attemptCount(),
            durationMillis);
    String key = sourceRecord.key() == null ? null : render(canonicalKey);
    ProducerRecord<String, String> output =
        new ProducerRecord<>(
            config.outputTopic(),
            null,
            sourceRecord.timestamp(),
            key,
            CanonicalJson.write(enriched));
    publish(output, "Enriched Record");
  }

  private void publishDeadLetter(
      SinkRecord sourceRecord,
      JsonNode canonicalKey,
      JsonNode canonicalValue,
      PermanentRecordException failure,
      long durationMillis) {
    String sourceId = sourceId(sourceRecord);
    String questionSetHash = DeterministicIds.questionSetHash(config.questions());
    String statePolicyHash = statePolicyHash();
    String requestedModel = config.getString(JevConnectorConfig.MODEL);
    String unresolvedModel = "unresolved:" + requestedModel;

    ObjectNode evaluation = CanonicalJson.MAPPER.createObjectNode();
    evaluation.put(
        "id",
        DeterministicIds.evaluationId(
            sourceId, questionSetHash, statePolicyHash, unresolvedModel));
    evaluation.set(
        "question_set",
        CanonicalJson.object(
            "id", config.getString(JevConnectorConfig.QUESTION_SET_ID), "hash", questionSetHash));
    evaluation.set(
        "state_policy",
        CanonicalJson.object(
            "mode", config.getString(JevConnectorConfig.STATE_MODE), "hash", statePolicyHash));
    if (failure.state() != null) {
      evaluation.set(
          "state",
          CanonicalJson.object("hash", DeterministicIds.evaluationStateHash(failure.state())));
    }
    evaluation.set(
        "model", CanonicalJson.object("requested", requestedModel, "resolved", unresolvedModel));
    evaluation.put("attempt_count", failure.attemptCount());
    evaluation.put("duration_ms", durationMillis);
    evaluation.put("failed_at", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

    ObjectNode deadLetter = CanonicalJson.MAPPER.createObjectNode();
    deadLetter.set("source", source(sourceRecord, canonicalKey, sourceId));
    deadLetter.set("input", canonicalValue);
    deadLetter.set("evaluation", evaluation);
    deadLetter.set(
        "error",
        CanonicalJson.object(
            "category", failure.category(),
            "code", failure.code(),
            "message", sanitize(failure.getMessage())));
    deadLetter.set("connector", connectorProvenance());

    publish(
        new ProducerRecord<>(
            config.deadLetterTopic(),
            null,
            sourceRecord.timestamp(),
            sourceId,
            CanonicalJson.write(deadLetter)),
        "Dead-Letter Record");
  }

  private void publish(ProducerRecord<String, String> record, String recordType) {
    try {
      producer.send(record).get();
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while publishing " + recordType, error);
    } catch (ExecutionException error) {
      throw new ConnectException("Failed to publish " + recordType, error.getCause());
    } catch (RuntimeException error) {
      throw new ConnectException("Failed to publish " + recordType, error);
    }
  }

  private EvaluationSuccess callJev(String state) {
    ObjectNode requestBody = CanonicalJson.MAPPER.createObjectNode();
    requestBody.put("state", state);
    requestBody.set("questions", config.questions());
    requestBody.put("model", config.getString(JevConnectorConfig.MODEL));
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(config.getString(JevConnectorConfig.ENDPOINT)))
            .timeout(Duration.ofMillis(config.getInt(JevConnectorConfig.REQUEST_TIMEOUT_MS)))
            .header("Authorization", "Bearer " + config.getPassword(JevConnectorConfig.API_KEY).value())
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    CanonicalJson.write(requestBody), StandardCharsets.UTF_8))
            .build();
    int maximumAttempts = config.getInt(JevConnectorConfig.RETRY_MAX_ATTEMPTS);
    for (int attempt = 1; attempt <= maximumAttempts; attempt++) {
      HttpResponse<String> response = null;
      try {
        response =
            http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        int status = response.statusCode();
        if (status == 200) {
          return new EvaluationSuccess(parseInferenceResult(response.body()), attempt);
        }
        if (status == 413) {
          throw new PermanentRecordException(
              "JEV_REQUEST",
              "RECORD_TOO_LARGE",
              "Jev rejected the Evaluation State as too large",
              state,
              attempt);
        }
        if (isTaskFatalStatus(status)) {
          throw new ConnectException("Jev rejected the request with HTTP " + status);
        }
        if (!isTransientStatus(status)) {
          throw new ConnectException("Jev returned an unrecognized HTTP status " + status);
        }
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while calling Jev", error);
      } catch (IOException transientFailure) {
        // Connection failures and per-attempt request timeouts are transient.
      }

      if (attempt == maximumAttempts) {
        if ("DLQ".equals(config.getString(JevConnectorConfig.TRANSIENT_EXHAUSTED))) {
          throw new PermanentRecordException(
              "JEV_SERVICE",
              "TRANSIENT_EXHAUSTED",
              "Jev Evaluation exhausted transient attempts",
              state,
              attempt);
        }
        throw new ConnectException(
            "Jev Evaluation exhausted " + maximumAttempts + " transient attempts");
      }
      sleepBeforeRetry(attempt, response == null ? EMPTY_HEADERS : response.headers());
    }
    throw new AssertionError("Configured Jev attempt limit was not applied");
  }

  private InferenceResult parseInferenceResult(String responseBody) {
    try {
      return InferenceResult.from(CanonicalJson.MAPPER.readTree(responseBody));
    } catch (ConnectException error) {
      throw error;
    } catch (Exception error) {
      throw new ConnectException("Jev response is not valid JSON");
    }
  }

  private void sleepBeforeRetry(int completedAttempt, HttpHeaders responseHeaders) {
    long localBackoff = exponentialBackoff(completedAttempt);
    long retryAfter = retryAfterMillis(responseHeaders);
    long delay = Math.max(localBackoff, retryAfter);
    try {
      sleeper.sleep(delay);
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while waiting to retry Jev", error);
    }
  }

  private long exponentialBackoff(int completedAttempt) {
    long initial = config.getLong(JevConnectorConfig.RETRY_INITIAL_BACKOFF_MS);
    long exponential = initial;
    for (int i = 1; i < completedAttempt; i++) {
      exponential = exponential > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : exponential * 2;
    }
    double jittered = exponential * boundedJitterMultiplier();
    return jittered >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0L, Math.round(jittered));
  }

  private double boundedJitterMultiplier() {
    return Math.max(0.5, Math.min(1.5, jitterMultiplier.getAsDouble()));
  }

  private long retryAfterMillis(HttpHeaders headers) {
    String configured = headers.firstValue("Retry-After").orElse(null);
    if (configured == null) {
      return 0;
    }
    long maximum = config.getLong(JevConnectorConfig.RETRY_MAX_RETRY_AFTER_MS);
    long requested;
    try {
      long seconds = Long.parseLong(configured.strip());
      if (seconds < 0) {
        return 0;
      }
      requested = seconds > maximum / 1000L ? maximum : seconds * 1000L;
    } catch (NumberFormatException notDeltaSeconds) {
      try {
        requested =
            Math.max(
                0L,
                Duration.between(
                        Instant.now(),
                        ZonedDateTime.parse(configured, DateTimeFormatter.RFC_1123_DATE_TIME)
                            .toInstant())
                    .toMillis());
      } catch (DateTimeParseException invalidHeader) {
        return 0;
      }
    }
    return Math.min(requested, maximum);
  }

  private static boolean isTaskFatalStatus(int status) {
    return status == 400 || status == 401 || status == 403 || status == 404 || status == 422;
  }

  private static boolean isTransientStatus(int status) {
    return status == 408 || status == 429 || status == 529 || (status >= 500 && status <= 599);
  }

  private ObjectNode enrichedRecord(
      SinkRecord sourceRecord,
      JsonNode canonicalKey,
      JsonNode canonicalValue,
      String state,
      InferenceResult inferenceResult,
      int attemptCount,
      long durationMillis) {
    String sourceId = sourceId(sourceRecord);
    String questionSetHash = DeterministicIds.questionSetHash(config.questions());
    String statePolicyHash = statePolicyHash();
    String resolvedModel = inferenceResult.resolvedModel();
    String evaluationId =
        DeterministicIds.evaluationId(
            sourceId, questionSetHash, statePolicyHash, resolvedModel);

    ObjectNode source = source(sourceRecord, canonicalKey, sourceId);

    ObjectNode evaluation = CanonicalJson.MAPPER.createObjectNode();
    evaluation.put("id", evaluationId);
    evaluation.set(
        "question_set",
        CanonicalJson.object(
            "id", config.getString(JevConnectorConfig.QUESTION_SET_ID), "hash", questionSetHash));
    evaluation.set(
        "state_policy",
        CanonicalJson.object(
            "mode", config.getString(JevConnectorConfig.STATE_MODE), "hash", statePolicyHash));
    evaluation.set("state", CanonicalJson.object("hash", DeterministicIds.evaluationStateHash(state)));
    evaluation.set(
        "model",
        CanonicalJson.object(
            "requested", config.getString(JevConnectorConfig.MODEL), "resolved", resolvedModel));
    evaluation.put("attempt_count", attemptCount);
    evaluation.put("duration_ms", durationMillis);
    evaluation.put("completed_at", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

    ObjectNode enriched = CanonicalJson.MAPPER.createObjectNode();
    enriched.set("source", source);
    enriched.set("input", canonicalValue);
    enriched.set("evaluation", evaluation);
    enriched.set("connector", connectorProvenance());
    enriched.set("jev", inferenceResult.json());
    return enriched;
  }

  private String fullValueState(SinkRecord sourceRecord, JsonNode canonicalValue) {
    if (sourceRecord.value() == null) {
      throw new ConnectException("Null Source Record values require a configured tombstone policy");
    }
    if (isRawBytes(sourceRecord.valueSchema(), sourceRecord.value())) {
      if (!"UTF-8".equals(config.getString(JevConnectorConfig.RAW_BYTES_ENCODING))) {
        throw new PermanentRecordException(
            "STATE_BUILDING",
            "RAW_BYTES_DISABLED",
            "Raw byte Evaluation State requires state.raw_bytes.encoding=UTF-8");
      }
      return decodeUtf8(sourceRecord.value());
    }
    return render(canonicalValue);
  }

  private String evaluationState(
      SinkRecord sourceRecord, JsonNode canonicalKey, JsonNode canonicalValue) {
    if (config.stateTemplate() != null) {
      return config.stateTemplate().render(sourceRecord, canonicalKey, canonicalValue);
    }
    return fullValueState(sourceRecord, canonicalValue);
  }

  private static boolean isRawBytes(Schema schema, Object value) {
    return (schema != null && schema.type() == Schema.Type.BYTES)
        || value instanceof byte[]
        || value instanceof ByteBuffer;
  }

  private static String decodeUtf8(Object value) {
    ByteBuffer bytes;
    if (value instanceof byte[] array) {
      bytes = ByteBuffer.wrap(array);
    } else {
      bytes = ((ByteBuffer) value).duplicate();
    }
    try {
      return StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(bytes)
          .toString();
    } catch (CharacterCodingException error) {
      throw new PermanentRecordException(
          "STATE_BUILDING", "INVALID_UTF8", "Raw byte Evaluation State is not valid UTF-8");
    }
  }

  private static String render(JsonNode value) {
    if (value.isTextual()) {
      return value.textValue();
    }
    return CanonicalJson.serialize(value);
  }

  private long elapsedMillis(long startedAt) {
    return Math.max(1, (monotonicNanos.getAsLong() - startedAt + 999_999) / 1_000_000);
  }

  private static String sanitize(String message) {
    String sanitized =
        message
            .replaceAll("[\\p{Cntrl}&&[^\\r\\n\\t]]", "?")
            .replaceAll("[\\r\\n\\t]+", " ");
    return sanitized.length() <= 512 ? sanitized : sanitized.substring(0, 512);
  }

  private static String sourceId(SinkRecord sourceRecord) {
    return DeterministicIds.sourceId(
        sourceRecord.topic(), sourceRecord.kafkaPartition(), sourceRecord.kafkaOffset());
  }

  private String statePolicyHash() {
    return DeterministicIds.statePolicyHash(
        config.getString(JevConnectorConfig.STATE_MODE),
        config.stateTemplateForHash(),
        config.getString(JevConnectorConfig.RAW_BYTES_ENCODING));
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

  private ObjectNode connectorProvenance() {
    return CanonicalJson.object(
        "name", config.getString(JevConnectorConfig.NAME),
        "plugin", "kafka-jev-connector",
        "version", Version.VALUE);
  }

  static Map<String, Object> producerProperties(JevConnectorConfig config) {
    Properties properties = new Properties();
    properties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        config.usesCloudKafkaEndpoint()
            ? config.getString(JevConnectorConfig.KAFKA_ENDPOINT).trim()
            : config.getString(JevConnectorConfig.OUTPUT_BOOTSTRAP).trim());
    if (config.hasKafkaCredentials()) {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
      properties.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
              + escapeJaas(config.getPassword(JevConnectorConfig.KAFKA_API_KEY).value())
              + "\" password=\""
              + escapeJaas(config.getPassword(JevConnectorConfig.KAFKA_API_SECRET).value())
              + "\";");
    } else {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
    }
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put("allow.auto.create.topics", false);
    @SuppressWarnings({"unchecked", "rawtypes"})
    Map<String, Object> result = (Map) properties;
    return result;
  }

  private static String escapeJaas(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  @FunctionalInterface
  interface Sleeper {
    void sleep(long milliseconds) throws InterruptedException;
  }

  private record EvaluationSuccess(InferenceResult inferenceResult, int attemptCount) {}

}
