package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public final class JevSinkTask extends SinkTask {
  private final Function<Map<String, Object>, Producer<String, String>> producerFactory;
  private final ConnectValueCanonicalizer canonicalizer = new ConnectValueCanonicalizer();
  private JevConnectorConfig config;
  private Producer<String, String> producer;
  private HttpClient http;

  public JevSinkTask() {
    this(properties -> new KafkaProducer<>(properties));
  }

  JevSinkTask(Function<Map<String, Object>, Producer<String, String>> producerFactory) {
    this.producerFactory = producerFactory;
  }

  @Override
  public void start(Map<String, String> properties) {
    config = new JevConnectorConfig(properties);
    producer = producerFactory.apply(producerProperties(config));
    http =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    for (SinkRecord sourceRecord : records) {
      evaluateAndPublish(sourceRecord);
    }
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
    JsonNode canonicalValue =
        canonicalizer.canonicalize(sourceRecord.valueSchema(), sourceRecord.value());
    JsonNode canonicalKey = canonicalizer.canonicalize(sourceRecord.keySchema(), sourceRecord.key());
    String state = evaluationState(sourceRecord, canonicalKey, canonicalValue);

    long startedAt = System.nanoTime();
    JsonNode inferenceResult = callJev(state);
    long durationMillis = Math.max(1, (System.nanoTime() - startedAt + 999_999) / 1_000_000);
    ObjectNode enriched =
        enrichedRecord(
            sourceRecord, canonicalKey, canonicalValue, state, inferenceResult, durationMillis);
    String key = sourceRecord.key() == null ? null : render(canonicalKey);
    ProducerRecord<String, String> output =
        new ProducerRecord<>(
            config.getString(JevConnectorConfig.OUTPUT_TOPIC),
            null,
            sourceRecord.timestamp(),
            key,
            CanonicalJson.write(enriched));
    try {
      producer.send(output).get();
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while publishing Enriched Record", error);
    } catch (ExecutionException error) {
      throw new ConnectException("Failed to publish Enriched Record", error.getCause());
    }
  }

  private JsonNode callJev(String state) {
    ObjectNode requestBody = CanonicalJson.MAPPER.createObjectNode();
    requestBody.put("state", state);
    requestBody.set("questions", config.questions());
    requestBody.put("model", config.getString(JevConnectorConfig.MODEL));
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(config.getString(JevConnectorConfig.ENDPOINT)))
            .timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer " + config.getPassword(JevConnectorConfig.API_KEY).value())
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    CanonicalJson.write(requestBody), StandardCharsets.UTF_8))
            .build();
    try {
      HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      if (response.statusCode() != 200) {
        throw new ConnectException("Jev returned HTTP " + response.statusCode());
      }
      JsonNode result = CanonicalJson.MAPPER.readTree(response.body());
      JsonNode resolvedModel = result.path("model");
      if (!result.isObject() || !resolvedModel.isTextual() || resolvedModel.asText().isBlank()) {
        throw new ConnectException("Jev response must be a JSON object with a usable model");
      }
      return result;
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while calling Jev", error);
    } catch (ConnectException error) {
      throw error;
    } catch (Exception error) {
      throw new ConnectException("Failed to call Jev", error);
    }
  }

  private ObjectNode enrichedRecord(
      SinkRecord sourceRecord,
      JsonNode canonicalKey,
      JsonNode canonicalValue,
      String state,
      JsonNode inferenceResult,
      long durationMillis) {
    String sourceId =
        DeterministicIds.sourceId(
            sourceRecord.topic(), sourceRecord.kafkaPartition(), sourceRecord.kafkaOffset());
    String questionSetHash = DeterministicIds.questionSetHash(config.questions());
    String statePolicyHash =
        DeterministicIds.statePolicyHash(
            config.getString(JevConnectorConfig.STATE_MODE),
            config.stateTemplateForHash(),
            config.getString(JevConnectorConfig.RAW_BYTES_ENCODING));
    String resolvedModel = inferenceResult.path("model").asText();
    String evaluationId =
        DeterministicIds.evaluationId(
            sourceId, questionSetHash, statePolicyHash, resolvedModel);

    ObjectNode source = CanonicalJson.MAPPER.createObjectNode();
    source.put("id", sourceId);
    source.put("topic", sourceRecord.topic());
    source.put("partition", sourceRecord.kafkaPartition());
    source.put("offset", sourceRecord.kafkaOffset());
    if (sourceRecord.timestamp() == null) {
      source.putNull("timestamp");
    } else {
      source.put("timestamp", DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(sourceRecord.timestamp())));
    }
    source.set("key", canonicalKey);

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
    evaluation.put("attempt_count", 1);
    evaluation.put("duration_ms", durationMillis);
    evaluation.put("completed_at", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

    ObjectNode enriched = CanonicalJson.MAPPER.createObjectNode();
    enriched.set("source", source);
    enriched.set("input", canonicalValue);
    enriched.set("evaluation", evaluation);
    enriched.set(
        "connector",
        CanonicalJson.object(
            "name", config.getString(JevConnectorConfig.NAME),
            "plugin", "kafka-jev-connector",
            "version", Version.VALUE));
    enriched.set("jev", inferenceResult);
    return enriched;
  }

  private String fullValueState(SinkRecord sourceRecord, JsonNode canonicalValue) {
    if (sourceRecord.value() == null) {
      throw new ConnectException("Null Source Record values require a configured tombstone policy");
    }
    if (isRawBytes(sourceRecord.valueSchema(), sourceRecord.value())) {
      if (!"UTF-8".equals(config.getString(JevConnectorConfig.RAW_BYTES_ENCODING))) {
        throw new ConnectException(
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
      throw new ConnectException("Raw byte Evaluation State is not valid UTF-8", error);
    }
  }

  private static String render(JsonNode value) {
    if (value.isTextual()) {
      return value.textValue();
    }
    return CanonicalJson.serialize(value);
  }

  private static Map<String, Object> producerProperties(JevConnectorConfig config) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(JevConnectorConfig.OUTPUT_BOOTSTRAP));
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put("allow.auto.create.topics", false);
    @SuppressWarnings({"unchecked", "rawtypes"})
    Map<String, Object> result = (Map) properties;
    return result;
  }

}
