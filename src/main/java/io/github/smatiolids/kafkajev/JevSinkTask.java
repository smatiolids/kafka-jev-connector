package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HexFormat;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public final class JevSinkTask extends SinkTask {
  private static final ObjectMapper JSON =
      JsonMapperFactory.create();

  private final Function<Map<String, Object>, Producer<String, String>> producerFactory;
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
    if (!(sourceRecord.value() instanceof String state)) {
      throw new ConnectException("FULL_VALUE currently requires a string Source Record value");
    }

    long startedAt = System.nanoTime();
    JsonNode inferenceResult = callJev(state);
    long durationMillis = Math.max(1, (System.nanoTime() - startedAt + 999_999) / 1_000_000);
    ObjectNode enriched = enrichedRecord(sourceRecord, state, inferenceResult, durationMillis);
    String key = sourceRecord.key() == null ? null : String.valueOf(sourceRecord.key());
    ProducerRecord<String, String> output =
        new ProducerRecord<>(
            config.getString(JevConnectorConfig.OUTPUT_TOPIC),
            null,
            sourceRecord.timestamp(),
            key,
            write(enriched));
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
    ObjectNode requestBody = JSON.createObjectNode();
    requestBody.put("state", state);
    requestBody.set("questions", config.questions());
    requestBody.put("model", config.getString(JevConnectorConfig.MODEL));
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(config.getString(JevConnectorConfig.ENDPOINT)))
            .timeout(Duration.ofSeconds(10))
            .header("Authorization", "Bearer " + config.getPassword(JevConnectorConfig.API_KEY).value())
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(write(requestBody), StandardCharsets.UTF_8))
            .build();
    try {
      HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      if (response.statusCode() != 200) {
        throw new ConnectException("Jev returned HTTP " + response.statusCode());
      }
      JsonNode result = JSON.readTree(response.body());
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
      SinkRecord sourceRecord, String state, JsonNode inferenceResult, long durationMillis) {
    String sourceId =
        hash(
            object(
                "topic", sourceRecord.topic(),
                "partition", sourceRecord.kafkaPartition(),
                "offset", sourceRecord.kafkaOffset()));
    String questionSetHash = hash(config.questions());
    JsonNode statePolicy =
        object("mode", "FULL_VALUE", "template", null, "raw_bytes_encoding", "DISABLED");
    String statePolicyHash = hash(statePolicy);
    String resolvedModel = inferenceResult.path("model").asText();
    String evaluationId =
        hash(
            object(
                "source_id", sourceId,
                "question_set_hash", questionSetHash,
                "state_policy_hash", statePolicyHash,
                "effective_model", resolvedModel));

    ObjectNode source = JSON.createObjectNode();
    source.put("id", sourceId);
    source.put("topic", sourceRecord.topic());
    source.put("partition", sourceRecord.kafkaPartition());
    source.put("offset", sourceRecord.kafkaOffset());
    if (sourceRecord.timestamp() == null) {
      source.putNull("timestamp");
    } else {
      source.put("timestamp", DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(sourceRecord.timestamp())));
    }
    if (sourceRecord.key() == null) {
      source.putNull("key");
    } else {
      source.put("key", String.valueOf(sourceRecord.key()));
    }

    ObjectNode evaluation = JSON.createObjectNode();
    evaluation.put("id", evaluationId);
    evaluation.set(
        "question_set",
        object("id", config.getString(JevConnectorConfig.QUESTION_SET_ID), "hash", questionSetHash));
    evaluation.set("state_policy", object("mode", "FULL_VALUE", "hash", statePolicyHash));
    evaluation.set("state", object("hash", hashText(state)));
    evaluation.set(
        "model",
        object(
            "requested", config.getString(JevConnectorConfig.MODEL), "resolved", resolvedModel));
    evaluation.put("attempt_count", 1);
    evaluation.put("duration_ms", durationMillis);
    evaluation.put("completed_at", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

    ObjectNode enriched = JSON.createObjectNode();
    enriched.set("source", source);
    enriched.put("input", state);
    enriched.set("evaluation", evaluation);
    enriched.set(
        "connector",
        object(
            "name", config.getString(JevConnectorConfig.NAME),
            "plugin", "kafka-jev-connector",
            "version", Version.VALUE));
    enriched.set("jev", inferenceResult);
    return enriched;
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

  private static ObjectNode object(Object... values) {
    ObjectNode object = JSON.createObjectNode();
    for (int index = 0; index < values.length; index += 2) {
      String name = (String) values[index];
      Object value = values[index + 1];
      if (value == null) {
        object.putNull(name);
      } else if (value instanceof Integer integer) {
        object.put(name, integer);
      } else if (value instanceof Long longValue) {
        object.put(name, longValue);
      } else {
        object.put(name, String.valueOf(value));
      }
    }
    return object;
  }

  private static String hash(JsonNode value) {
    return hashText(write(canonicalize(value)));
  }

  private static JsonNode canonicalize(JsonNode value) {
    if (value.isObject()) {
      ObjectNode sorted = JSON.createObjectNode();
      TreeMap<String, JsonNode> fields = new TreeMap<>();
      value.fields().forEachRemaining(entry -> fields.put(entry.getKey(), entry.getValue()));
      fields.forEach((name, child) -> sorted.set(name, canonicalize(child)));
      return sorted;
    }
    if (value.isArray()) {
      ArrayNode array = JSON.createArrayNode();
      value.forEach(child -> array.add(canonicalize(child)));
      return array;
    }
    return value;
  }

  private static String hashText(String value) {
    try {
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      return "sha256:" + HexFormat.of().formatHex(digest);
    } catch (Exception error) {
      throw new IllegalStateException("SHA-256 is unavailable", error);
    }
  }

  private static String write(JsonNode value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (Exception error) {
      throw new ConnectException("Could not serialize JSON", error);
    }
  }

  private static final class JsonMapperFactory {
    private JsonMapperFactory() {}

    private static ObjectMapper create() {
      ObjectMapper mapper = new ObjectMapper();
      mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
      mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
      return mapper;
    }
  }
}
