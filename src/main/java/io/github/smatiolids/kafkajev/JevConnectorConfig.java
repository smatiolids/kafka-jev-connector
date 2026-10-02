package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

final class JevConnectorConfig extends AbstractConfig {
  static final String NAME = "name";
  static final String TOPICS = "topics";
  static final String OUTPUT_TOPIC = "output.topic";
  static final String DLQ_TOPIC = "errors.deadletter.topic";
  static final String QUESTION_SET_ID = "question.set.id";
  static final String API_KEY = "jev.api.key";
  static final String QUESTIONS = "jev.questions";
  static final String STATE_MODE = "state.mode";
  static final String STATE_TEMPLATE = "state.template";
  static final String RAW_BYTES_ENCODING = "state.raw_bytes.encoding";
  static final String STATE_MAX_BYTES = "state.max.bytes";
  static final String TOMBSTONE_BEHAVIOR = "behavior.on.null.values";
  static final String ENDPOINT = "jev.endpoint";
  static final String ALLOW_HTTP = "jev.allow.insecure.http";
  static final String MODEL = "jev.model";
  static final String MAX_IN_FLIGHT = "jev.max.in.flight";
  static final String CONNECT_TIMEOUT_MS = "jev.connect.timeout.ms";
  static final String REQUEST_TIMEOUT_MS = "jev.request.timeout.ms";
  static final String RETRY_MAX_ATTEMPTS = "jev.retry.max.attempts";
  static final String RETRY_INITIAL_BACKOFF_MS = "jev.retry.initial.backoff.ms";
  static final String RETRY_MAX_RETRY_AFTER_MS = "jev.retry.max.retry_after.ms";
  static final String TRANSIENT_EXHAUSTED = "errors.transient.exhausted";
  static final String OUTPUT_KEY_MODE = "output.key.mode";
  static final String OUTPUT_HEADERS_MODE = "output.headers.mode";
  static final String OUTPUT_BOOTSTRAP = "output.bootstrap.servers";
  static final String KAFKA_ENDPOINT = "kafka.endpoint";
  static final String KAFKA_API_KEY = "kafka.api.key";
  static final String KAFKA_API_SECRET = "kafka.api.secret";

  private static final ObjectMapper JSON = new ObjectMapper();

  static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Connector Instance name")
          .define(TOPICS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Explicit input topics")
          .define(OUTPUT_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Enriched Record topic")
          .define(DLQ_TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Dead-Letter Record topic")
          .define(QUESTION_SET_ID, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Question Set identifier")
          .define(API_KEY, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, "TypeSafe AI API key")
          .define(QUESTIONS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Jev questions JSON object")
          .define(
              STATE_MODE,
              ConfigDef.Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.ValidString.in("FULL_VALUE", "TEMPLATE"),
              ConfigDef.Importance.HIGH,
              "Evaluation State policy")
          .define(
              STATE_TEMPLATE,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "Compiled Evaluation State template")
          .define(
              RAW_BYTES_ENCODING,
              ConfigDef.Type.STRING,
              "DISABLED",
              ConfigDef.ValidString.in("DISABLED", "UTF-8"),
              ConfigDef.Importance.MEDIUM,
              "Raw byte Evaluation State encoding")
          .define(
              STATE_MAX_BYTES,
              ConfigDef.Type.LONG,
              0L,
              ConfigDef.Range.atLeast(0L),
              ConfigDef.Importance.MEDIUM,
              "Maximum Evaluation State UTF-8 bytes; zero disables the limit")
          .define(
              TOMBSTONE_BEHAVIOR,
              ConfigDef.Type.STRING,
              "IGNORE",
              ConfigDef.ValidString.in("IGNORE", "DLQ", "FAIL"),
              ConfigDef.Importance.MEDIUM,
              "Tombstone behavior")
          .define(
              ENDPOINT,
              ConfigDef.Type.STRING,
              "https://api.typesafe.ai/v1/systemone",
              ConfigDef.Importance.MEDIUM,
              "Jev endpoint")
          .define(ALLOW_HTTP, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "Allow local HTTP")
          .define(MODEL, ConfigDef.Type.STRING, "jev-latest", ConfigDef.Importance.MEDIUM, "Requested model")
          .define(
              MAX_IN_FLIGHT,
              ConfigDef.Type.INT,
              4,
              ConfigDef.Range.atLeast(1),
              ConfigDef.Importance.MEDIUM,
              "Maximum concurrent Jev requests per task")
          .define(
              CONNECT_TIMEOUT_MS,
              ConfigDef.Type.INT,
              5000,
              ConfigDef.Range.atLeast(1),
              ConfigDef.Importance.MEDIUM,
              "Jev connection timeout in milliseconds")
          .define(
              REQUEST_TIMEOUT_MS,
              ConfigDef.Type.INT,
              10000,
              ConfigDef.Range.atLeast(1),
              ConfigDef.Importance.MEDIUM,
              "Jev Evaluation Attempt timeout in milliseconds")
          .define(
              RETRY_MAX_ATTEMPTS,
              ConfigDef.Type.INT,
              3,
              ConfigDef.Range.atLeast(1),
              ConfigDef.Importance.MEDIUM,
              "Total Jev Evaluation Attempts")
          .define(
              RETRY_INITIAL_BACKOFF_MS,
              ConfigDef.Type.LONG,
              250L,
              ConfigDef.Range.atLeast(0L),
              ConfigDef.Importance.MEDIUM,
              "Initial retry backoff in milliseconds")
          .define(
              RETRY_MAX_RETRY_AFTER_MS,
              ConfigDef.Type.LONG,
              30000L,
              ConfigDef.Range.atLeast(0L),
              ConfigDef.Importance.MEDIUM,
              "Maximum accepted Retry-After delay in milliseconds")
          .define(
              TRANSIENT_EXHAUSTED,
              ConfigDef.Type.STRING,
              "FAIL",
              ConfigDef.ValidString.in("FAIL", "DLQ"),
              ConfigDef.Importance.MEDIUM,
              "Outcome after transient Jev attempts are exhausted")
          .define(
              OUTPUT_KEY_MODE,
              ConfigDef.Type.STRING,
              "ORIGINAL",
              ConfigDef.ValidString.in("ORIGINAL", "EVALUATION_ID"),
              ConfigDef.Importance.MEDIUM,
              "Enriched Record Kafka key policy")
          .define(
              OUTPUT_HEADERS_MODE,
              ConfigDef.Type.STRING,
              "NONE",
              ConfigDef.ValidString.in("NONE", "COPY"),
              ConfigDef.Importance.MEDIUM,
              "Source Record header propagation policy")
          .define(
              OUTPUT_BOOTSTRAP,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "Output Kafka bootstrap servers")
          .define(
              KAFKA_ENDPOINT,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "Confluent Cloud Kafka endpoint")
          .define(
              KAFKA_API_KEY,
              ConfigDef.Type.PASSWORD,
              null,
              ConfigDef.Importance.HIGH,
              "Kafka API key")
          .define(
              KAFKA_API_SECRET,
              ConfigDef.Type.PASSWORD,
              null,
              ConfigDef.Importance.HIGH,
              "Kafka API secret");

  private final CompiledStateTemplate stateTemplate;

  JevConnectorConfig(Map<String, ?> properties) {
    super(CONFIG_DEF, properties);
    validateRequiredValues();
    validateTopics();
    validateQuestions();
    validateEndpoint();
    validateStatePolicy();
    validateKafkaConnection();
    stateTemplate =
        "TEMPLATE".equals(getString(STATE_MODE))
            ? CompiledStateTemplate.compile(getString(STATE_TEMPLATE))
            : null;
  }

  CompiledStateTemplate stateTemplate() {
    return stateTemplate;
  }

  String stateTemplateForHash() {
    return stateTemplate == null ? null : getString(STATE_TEMPLATE);
  }

  JsonNode questions() {
    try {
      return JSON.readTree(getString(QUESTIONS));
    } catch (Exception error) {
      throw new ConfigException(QUESTIONS, null, "must be a JSON object");
    }
  }

  Set<String> inputTopics() {
    LinkedHashSet<String> topics = new LinkedHashSet<>();
    for (String topic : getString(TOPICS).split(",", -1)) {
      topics.add(topic.trim());
    }
    return Set.copyOf(topics);
  }

  Set<String> allTopics() {
    LinkedHashSet<String> topics = new LinkedHashSet<>(inputTopics());
    topics.add(getString(OUTPUT_TOPIC).trim());
    topics.add(getString(DLQ_TOPIC).trim());
    return Set.copyOf(topics);
  }

  String outputTopic() {
    return getString(OUTPUT_TOPIC).trim();
  }

  String deadLetterTopic() {
    return getString(DLQ_TOPIC).trim();
  }

  boolean usesCloudKafkaEndpoint() {
    return hasText(getString(KAFKA_ENDPOINT));
  }

  boolean hasKafkaCredentials() {
    return getPassword(KAFKA_API_KEY) != null && getPassword(KAFKA_API_SECRET) != null;
  }

  private void validateRequiredValues() {
    for (String key :
        Set.of(
            NAME,
            TOPICS,
            OUTPUT_TOPIC,
            DLQ_TOPIC,
            QUESTION_SET_ID,
            QUESTIONS,
            STATE_MODE,
            MODEL)) {
      if (!hasText(getString(key))) {
        throw new ConfigException(key, null, "must be non-empty");
      }
    }
    if (getPassword(API_KEY) == null || !hasText(getPassword(API_KEY).value())) {
      throw new ConfigException(API_KEY, null, "must be non-empty");
    }
  }

  private void validateTopics() {
    if (originals().containsKey("topics.regex")) {
      throw new ConfigException("topics.regex is not supported; configure explicit topics");
    }
    String[] configuredInputs = getString(TOPICS).split(",", -1);
    Set<String> inputTopics = new HashSet<>();
    for (String configured : configuredInputs) {
      String topic = configured.trim();
      if (topic.isEmpty() || !inputTopics.add(topic)) {
        throw new ConfigException(TOPICS, null, "must contain unique, non-empty explicit topics");
      }
    }
    String output = getString(OUTPUT_TOPIC).trim();
    String dlq = getString(DLQ_TOPIC).trim();
    if (inputTopics.contains(output) || inputTopics.contains(dlq) || output.equals(dlq)) {
      throw new ConfigException("Input, output, and dead-letter topics must be distinct");
    }
  }

  private void validateQuestions() {
    JsonNode configured = questions();
    if (!configured.isObject()) {
      throw new ConfigException(QUESTIONS, getString(QUESTIONS), "must be a JSON object");
    }
  }

  private void validateEndpoint() {
    URI endpoint;
    try {
      endpoint = URI.create(getString(ENDPOINT));
    } catch (IllegalArgumentException error) {
      throw new ConfigException(ENDPOINT, getString(ENDPOINT), "must be an absolute HTTP(S) URI");
    }
    boolean http = "http".equalsIgnoreCase(endpoint.getScheme());
    boolean https = "https".equalsIgnoreCase(endpoint.getScheme());
    if (!endpoint.isAbsolute()
        || endpoint.getHost() == null
        || endpoint.getUserInfo() != null
        || (!http && !https)
        || (http && !getBoolean(ALLOW_HTTP))) {
      throw new ConfigException(ENDPOINT, endpoint, "must use HTTPS unless insecure HTTP is explicitly enabled");
    }
  }

  private void validateStatePolicy() {
    boolean configuredTemplate = originals().containsKey(STATE_TEMPLATE);
    if ("TEMPLATE".equals(getString(STATE_MODE))) {
      if (!configuredTemplate || !hasText(getString(STATE_TEMPLATE))) {
        throw new ConfigException(STATE_TEMPLATE, null, "is required for TEMPLATE state mode");
      }
    } else if (configuredTemplate) {
      throw new ConfigException(STATE_TEMPLATE, null, "is only valid for TEMPLATE state mode");
    }
  }

  private void validateKafkaConnection() {
    boolean local = hasText(getString(OUTPUT_BOOTSTRAP));
    boolean cloud = hasText(getString(KAFKA_ENDPOINT));
    boolean key = getPassword(KAFKA_API_KEY) != null && hasText(getPassword(KAFKA_API_KEY).value());
    boolean secret =
        getPassword(KAFKA_API_SECRET) != null && hasText(getPassword(KAFKA_API_SECRET).value());
    if (local == cloud) {
      throw new ConfigException(
          "Configure exactly one of output.bootstrap.servers or kafka.endpoint");
    }
    if (key != secret || (cloud && !key)) {
      throw new ConfigException("Kafka API key and secret must be configured together");
    }
  }

  private static boolean hasText(String value) {
    return value != null && !value.isBlank();
  }
}
