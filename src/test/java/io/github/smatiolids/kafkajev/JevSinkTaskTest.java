package io.github.smatiolids.kafkajev;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JevSinkTaskTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  private HttpServer fakeJev;
  private ExecutorService fakeJevExecutor;
  private final AtomicReference<String> resolvedModel = new AtomicReference<>("jev-1.13.0");

  @AfterEach
  void stopFakeJev() {
    if (fakeJev != null) {
      fakeJev.stop(0);
    }
    if (fakeJevExecutor != null) {
      fakeJevExecutor.shutdownNow();
    }
  }

  @Test
  void publishesAnEnrichedRecordForAStringSourceRecord() throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    MockProducer<String, String> output =
        new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
    JevSinkTask task = new JevSinkTask(ignored -> output);
    Map<String, String> connectorConfig =
        Map.ofEntries(
            Map.entry("name", "support-evaluator"),
            Map.entry("topics", "support-input"),
            Map.entry("output.topic", "support-output"),
            Map.entry("errors.deadletter.topic", "support-dlq"),
            Map.entry("question.set.id", "support-routing/v1"),
            Map.entry("jev.api.key", "test-key"),
            Map.entry(
                "jev.questions",
                "{\"department\":{\"type\":\"choice\",\"instructions\":\"Route it\"}}"),
            Map.entry("state.mode", "FULL_VALUE"),
            Map.entry("jev.model", "jev-latest"),
            Map.entry("jev.endpoint", endpoint()),
            Map.entry("jev.allow.insecure.http", "true"),
            Map.entry("output.bootstrap.servers", "unused:9092"));
    JevSinkConnector connector = new JevSinkConnector((topics, properties) -> {});
    connector.start(connectorConfig);
    assertEquals(JevSinkTask.class, connector.taskClass());
    assertEquals(connectorConfig, connector.taskConfigs(1).get(0));

    task.start(connectorConfig);

    task.put(
        List.of(
            new SinkRecord(
                "support-input",
                2,
                null,
                "customer-42",
                null,
                "Please help, this is urgent",
                184,
                Instant.parse("2026-10-02T13:45:12.345Z").toEpochMilli(),
                TimestampType.CREATE_TIME)));

    JsonNode request = receivedRequest.get();
    assertEquals("Please help, this is urgent", request.path("state").asText());
    assertEquals("jev-latest", request.path("model").asText());
    assertEquals("choice", request.path("questions").path("department").path("type").asText());

    assertEquals(1, output.history().size());
    var published = output.history().get(0);
    assertEquals("support-output", published.topic());
    assertEquals("customer-42", published.key());
    assertEquals(Instant.parse("2026-10-02T13:45:12.345Z").toEpochMilli(), published.timestamp());

    JsonNode enriched = JSON.readTree(published.value());
    assertEquals("support-input", enriched.at("/source/topic").asText());
    assertEquals(2, enriched.at("/source/partition").asInt());
    assertEquals(184, enriched.at("/source/offset").asLong());
    assertEquals("customer-42", enriched.at("/source/key").asText());
    assertEquals(
        "sha256:7e9d14274c25fe070ce7f8f22a826d69e6285baf36c0741fdfda2771f5ea3c63",
        enriched.at("/source/id").asText());
    assertEquals("Please help, this is urgent", enriched.path("input").asText());
    assertEquals("support-routing/v1", enriched.at("/evaluation/question_set/id").asText());
    assertEquals(
        "sha256:5a10257bfee1e7c1bc05138e9561afec0e7891e2895b8bc185600cfb1a2aa669",
        enriched.at("/evaluation/question_set/hash").asText());
    assertEquals("FULL_VALUE", enriched.at("/evaluation/state_policy/mode").asText());
    assertEquals(
        "sha256:73610c3a5bebbe3c5cce82d0621f0d4c65b2a73a78a14e5bbdbd311add1915fa",
        enriched.at("/evaluation/state_policy/hash").asText());
    assertEquals(
        "sha256:74420916defec50c4a863822ad1b0ddc05c7828b694808a2aff8795082eac319",
        enriched.at("/evaluation/state/hash").asText());
    assertEquals("jev-latest", enriched.at("/evaluation/model/requested").asText());
    assertEquals("jev-1.13.0", enriched.at("/evaluation/model/resolved").asText());
    assertEquals(1, enriched.at("/evaluation/attempt_count").asInt());
    assertNotEquals(0, enriched.at("/evaluation/duration_ms").asLong());
    assertEquals("support-evaluator", enriched.at("/connector/name").asText());
    assertEquals(
        JSON.readTree(
            "{\"model\":\"jev-1.13.0\",\"answers\":{\"department\":\"technical\"},\"usage\":{\"provider\":\"fake\"}}"),
        enriched.path("jev"));

    task.stop();
    connector.stop();
  }

  @Test
  void evaluationIdKeyModeUsesTheDeterministicEvaluationId() throws Exception {
    fakeJev = fakeJev(new AtomicReference<>());
    Map<String, String> evaluationIdKeys = new java.util.HashMap<>(config());
    evaluationIdKeys.put("output.key.mode", "EVALUATION_ID");
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(evaluationIdKeys);

    task.put(List.of(sourceRecord(null, Map.of("tenant", "acme"), null, "same source")));

    ProducerRecord<String, String> published = output.history().get(0);
    JsonNode enriched = JSON.readTree(published.value());
    assertEquals(enriched.at("/evaluation/id").asText(), published.key());
    assertEquals(
        "sha256:dcc2ee43358f5b0068a0ad5d2e499b306524c96d74c167f60def5dce019e3fa9",
        published.key());
    task.stop();
  }

  @Test
  void inputHeadersAreOptInWhileConnectorProvenanceIsAlwaysPresent() throws Exception {
    fakeJev = fakeJev(new AtomicReference<>());
    ConnectHeaders sourceHeaders = new ConnectHeaders();
    sourceHeaders.addString("trace-id", "trace-123");
    sourceHeaders.addString("kafka-jev-evaluation-id", "untrusted-input");
    SinkRecord source =
        new SinkRecord(
            "support-input",
            2,
            null,
            "customer-42",
            null,
            "same source",
            184,
            Instant.parse("2026-10-02T13:45:12.345Z").toEpochMilli(),
            TimestampType.CREATE_TIME,
            sourceHeaders);

    MockProducer<String, String> defaultOutput = output();
    JevSinkTask defaultTask = new JevSinkTask(ignored -> defaultOutput);
    defaultTask.start(config());
    defaultTask.put(List.of(source));

    ProducerRecord<String, String> defaultRecord = defaultOutput.history().get(0);
    JsonNode defaultBody = JSON.readTree(defaultRecord.value());
    assertEquals(null, defaultRecord.headers().lastHeader("trace-id"));
    assertEquals(
        defaultBody.at("/evaluation/id").asText(),
        utf8(defaultRecord.headers().lastHeader("kafka-jev-evaluation-id").value()));
    assertEquals(
        "jev-1.13.0",
        utf8(defaultRecord.headers().lastHeader("kafka-jev-resolved-model").value()));
    defaultTask.stop();

    Map<String, String> copying = new java.util.HashMap<>(config());
    copying.put("output.headers.mode", "COPY");
    MockProducer<String, String> copyOutput = output();
    JevSinkTask copyTask = new JevSinkTask(ignored -> copyOutput);
    copyTask.start(copying);
    copyTask.put(List.of(source));

    ProducerRecord<String, String> copiedRecord = copyOutput.history().get(0);
    JsonNode copiedBody = JSON.readTree(copiedRecord.value());
    assertEquals("trace-123", utf8(copiedRecord.headers().lastHeader("trace-id").value()));
    assertEquals(
        copiedBody.at("/evaluation/id").asText(),
        utf8(copiedRecord.headers().lastHeader("kafka-jev-evaluation-id").value()));
    assertEquals(
        2,
        java.util.stream.StreamSupport.stream(
                copiedRecord.headers().headers("kafka-jev-evaluation-id").spliterator(), false)
            .count());
    copyTask.stop();
  }

  @Test
  void deadLetterKeyTimestampAndProvenanceIgnoreEnrichedOutputPolicies() throws Exception {
    Map<String, String> configured = new java.util.HashMap<>(requiredTemplateConfig());
    configured.put("output.key.mode", "EVALUATION_ID");
    configured.put("output.headers.mode", "COPY");
    ConnectHeaders sourceHeaders = new ConnectHeaders();
    sourceHeaders.addString("trace-id", "must-not-cross-the-dlq-boundary");
    long sourceTimestamp = Instant.parse("2026-10-02T13:45:12.345Z").toEpochMilli();
    SinkRecord source =
        new SinkRecord(
            "support-input",
            2,
            null,
            "customer-42",
            null,
            Map.of("message", "safe"),
            184,
            sourceTimestamp,
            TimestampType.CREATE_TIME,
            sourceHeaders);
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(configured);

    task.put(List.of(source));

    ProducerRecord<String, String> published = output.history().get(0);
    JsonNode deadLetter = JSON.readTree(published.value());
    assertEquals("support-dlq", published.topic());
    assertEquals(deadLetter.at("/source/id").asText(), published.key());
    assertNotEquals(deadLetter.at("/evaluation/id").asText(), published.key());
    assertEquals(sourceTimestamp, published.timestamp());
    assertEquals(null, published.headers().lastHeader("trace-id"));
    assertEquals(
        deadLetter.at("/evaluation/id").asText(),
        utf8(published.headers().lastHeader("kafka-jev-evaluation-id").value()));
    assertEquals(
        "unresolved:jev-latest",
        utf8(published.headers().lastHeader("kafka-jev-resolved-model").value()));
    task.stop();
  }

  @Test
  void rejectsUnknownOutputKeyAndHeaderModesAtStartup() {
    Map<String, String> invalidKeyMode = new java.util.HashMap<>(requiredTemplateConfig());
    invalidKeyMode.put("output.key.mode", "SOURCE_ID");
    assertThrows(
        ConfigException.class,
        () -> new JevSinkTask(ignored -> output()).start(invalidKeyMode));

    Map<String, String> invalidHeaderMode = new java.util.HashMap<>(requiredTemplateConfig());
    invalidHeaderMode.put("output.headers.mode", "ALL");
    assertThrows(
        ConfigException.class,
        () -> new JevSinkTask(ignored -> output()).start(invalidHeaderMode));
  }

  @Test
  void canonicalizesConnectValuesAndPublishesFixedProvenanceVectors() throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(config());

    Schema schema =
        SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("count", Schema.INT32_SCHEMA)
            .field("active", Schema.BOOLEAN_SCHEMA)
            .field("amount", Decimal.schema(2))
            .field("date", org.apache.kafka.connect.data.Date.SCHEMA)
            .field("time", org.apache.kafka.connect.data.Time.SCHEMA)
            .field("timestamp", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .field("items", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("attributes", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
            .field("codes", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
            .field("binary", Schema.BYTES_SCHEMA)
            .build();
    Map<Integer, String> codes = new LinkedHashMap<>();
    codes.put(2, "two");
    codes.put(1, "one");
    Struct value =
        new Struct(schema)
            .put("name", "Ada")
            .put("count", 7)
            .put("active", true)
            .put("amount", new BigDecimal("1234.50"))
            .put("date", Date.from(Instant.parse("2026-10-02T00:00:00Z")))
            .put("time", Date.from(Instant.parse("1970-01-01T13:45:12.345Z")))
            .put("timestamp", Date.from(Instant.parse("2026-10-02T13:45:12.345Z")))
            .put("items", List.of("first", "second"))
            .put("attributes", Map.of("z", 2, "a", 1))
            .put("codes", codes)
            .put("binary", "hi".getBytes(StandardCharsets.UTF_8));
    Schema keySchema = SchemaBuilder.struct().field("tenant", Schema.STRING_SCHEMA).build();
    Struct key = new Struct(keySchema).put("tenant", "acme");

    task.put(List.of(sourceRecord(keySchema, key, schema, value)));

    String canonicalValue =
        "{\"active\":true,\"amount\":\"1234.50\",\"attributes\":{\"a\":1,\"z\":2},"
            + "\"binary\":{\"$type\":\"bytes\",\"base64\":\"aGk=\"},\"codes\":{\"$type\":\"map\","
            + "\"entries\":[{\"key\":1,\"value\":\"one\"},{\"key\":2,\"value\":\"two\"}]},"
            + "\"count\":7,\"date\":\"2026-10-02\",\"items\":[\"first\",\"second\"],\"name\":\"Ada\","
            + "\"time\":\"13:45:12.345\",\"timestamp\":\"2026-10-02T13:45:12.345Z\"}";
    assertEquals(canonicalValue, receivedRequest.get().path("state").asText());

    JsonNode enriched = JSON.readTree(output.history().get(0).value());
    assertEquals(JSON.readTree(canonicalValue), enriched.path("input"));
    assertEquals(JSON.readTree("{\"tenant\":\"acme\"}"), enriched.at("/source/key"));
    assertEquals("{\"tenant\":\"acme\"}", output.history().get(0).key());
    assertEquals(
        "sha256:7e9d14274c25fe070ce7f8f22a826d69e6285baf36c0741fdfda2771f5ea3c63",
        enriched.at("/source/id").asText());
    assertEquals(
        "sha256:5a10257bfee1e7c1bc05138e9561afec0e7891e2895b8bc185600cfb1a2aa669",
        enriched.at("/evaluation/question_set/hash").asText());
    assertEquals(
        "sha256:73610c3a5bebbe3c5cce82d0621f0d4c65b2a73a78a14e5bbdbd311add1915fa",
        enriched.at("/evaluation/state_policy/hash").asText());
    assertEquals(
        "sha256:dd8af5b89c422b757f36d3b6c0168ec1b1954a4316dfad6b4a59b0fbab6ff9b9",
        enriched.at("/evaluation/state/hash").asText());
    assertEquals(
        "sha256:dcc2ee43358f5b0068a0ad5d2e499b306524c96d74c167f60def5dce019e3fa9",
        enriched.at("/evaluation/id").asText());
    task.stop();
  }

  @Test
  void rendersEveryKafkaConnectScalarAsFullValueState() throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    JevSinkTask task = new JevSinkTask(ignored -> output());
    task.start(config());

    assertFullValueState(task, receivedRequest, Schema.INT8_SCHEMA, (byte) 7, "7");
    assertFullValueState(task, receivedRequest, Schema.INT16_SCHEMA, (short) -12, "-12");
    assertFullValueState(task, receivedRequest, Schema.INT32_SCHEMA, 42, "42");
    assertFullValueState(
        task, receivedRequest, Schema.INT64_SCHEMA, 9_007_199_254_740_991L, "9007199254740991");
    assertFullValueState(task, receivedRequest, Schema.FLOAT32_SCHEMA, 1.5f, "1.5");
    assertFullValueState(task, receivedRequest, Schema.FLOAT64_SCHEMA, 0.000001d, "0.000001");
    assertFullValueState(task, receivedRequest, Schema.BOOLEAN_SCHEMA, false, "false");
    assertFullValueState(task, receivedRequest, Schema.STRING_SCHEMA, "unquoted text", "unquoted text");

    task.stop();
  }

  @Test
  void sendsTheExactAllowListedTemplateStateToJev() throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    Map<String, String> templateConfig = new java.util.HashMap<>(config());
    templateConfig.put("state.mode", "TEMPLATE");
    templateConfig.put(
        "state.template",
        "customer=${value:/customer/name}; key=${key:/tenant~1id}; tilde=${value:/til~0de}; "
            + "whole-key=${key}; whole-value=${value}; items=${value:/items}; "
            + "empty=[${value:/empty:-fallback}]; false=${value:/active:-fallback}; "
            + "zero=${value:/count:-fallback}; null=${value:/nullable:-fallback}; "
            + "missing=${value:/missing:-fallback}; trace=${header:trace-id}; "
            + "absent=${header:not-there:-none}; source=${metadata:topic}/${metadata:partition}/"
            + "${metadata:offset}@${metadata:timestamp}; escaped=\\${literal}; slash=\\\\; "
            + "brace=${value:/missing:-right\\}}");
    task.start(templateConfig);

    ConnectHeaders headers = new ConnectHeaders();
    headers.addBytes("trace-id", "old".getBytes(StandardCharsets.UTF_8));
    headers.addBytes("trace-id", "latest".getBytes(StandardCharsets.UTF_8));
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("customer", Map.of("name", "Ada"));
    value.put("items", List.of("first", Map.of("b", 2, "a", 1)));
    value.put("til~de", "escaped-pointer");
    value.put("empty", "");
    value.put("active", false);
    value.put("count", 0);
    value.put("nullable", null);
    long timestamp = Instant.parse("2026-10-02T13:45:12.345Z").toEpochMilli();
    SinkRecord source =
        new SinkRecord(
            "support-input",
            2,
            null,
            Map.of("tenant/id", "acme"),
            null,
            value,
            184,
            timestamp,
            TimestampType.CREATE_TIME,
            headers);

    task.put(List.of(source));

    assertEquals(
        "customer=Ada; key=acme; tilde=escaped-pointer; whole-key={\"tenant/id\":\"acme\"}; "
            + "whole-value={\"active\":false,\"count\":0,\"customer\":{\"name\":\"Ada\"},"
            + "\"empty\":\"\",\"items\":[\"first\",{\"a\":1,\"b\":2}],\"nullable\":null,"
            + "\"til~de\":\"escaped-pointer\"}; items=[\"first\",{\"a\":1,\"b\":2}]; "
            + "empty=[]; false=false; zero=0; null=fallback; missing=fallback; trace=latest; "
            + "absent=none; source=support-input/2/184@" + timestamp
            + "; escaped=${literal}; slash=\\; brace=right}",
        receivedRequest.get().path("state").asText());
    JsonNode enriched = JSON.readTree(output.history().get(0).value());
    assertEquals("TEMPLATE", enriched.at("/evaluation/state_policy/mode").asText());
    assertEquals(
        DeterministicIds.statePolicyHash(
            "TEMPLATE", templateConfig.get("state.template"), "DISABLED"),
        enriched.at("/evaluation/state_policy/hash").asText());
    task.stop();
  }

  @Test
  void usesAHeaderDefaultForInvalidUtf8AndFailsWithoutOne() throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    ConnectHeaders headers = new ConnectHeaders();
    headers.addBytes("trace-id", new byte[] {(byte) 0xc3, 0x28});
    SinkRecord source =
        new SinkRecord(
            "support-input", 2, null, null, null, Map.of("message", "safe"), 184,
            Instant.parse("2026-10-02T13:45:12.345Z").toEpochMilli(),
            TimestampType.CREATE_TIME, headers);

    Map<String, String> withDefault = new java.util.HashMap<>(config());
    withDefault.put("state.mode", "TEMPLATE");
    withDefault.put("state.template", "trace=${header:trace-id:-unavailable}");
    JevSinkTask defaultingTask = new JevSinkTask(ignored -> output());
    defaultingTask.start(withDefault);
    defaultingTask.put(List.of(source));
    assertEquals("trace=unavailable", receivedRequest.get().path("state").asText());
    defaultingTask.stop();

    Map<String, String> required = new java.util.HashMap<>(withDefault);
    required.put("state.template", "trace=${header:trace-id}");
    MockProducer<String, String> failingOutput = output();
    JevSinkTask failingTask = new JevSinkTask(ignored -> failingOutput);
    failingTask.start(required);
    failingTask.put(List.of(source));
    JsonNode deadLetter = JSON.readTree(failingOutput.history().get(0).value());
    assertEquals("INVALID_UTF8", deadLetter.at("/error/code").asText());
    assertEquals("Template header is not valid UTF-8", deadLetter.at("/error/message").asText());
    assertFalse(failingOutput.history().get(0).value().contains("trace-id"));
    failingTask.stop();
  }

  @Test
  void missingRequiredTemplateValuesPublishASanitizedDeadLetterRecord() throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    Map<String, String> required = new java.util.HashMap<>(config());
    required.put("state.mode", "TEMPLATE");
    required.put("state.template", "approved=${value:/approved}");
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(required);

    task.put(List.of(sourceRecord(null, "customer-42", null, Map.of("secret", "hidden"))));

    assertEquals(null, receivedRequest.get());
    assertEquals(1, output.history().size());
    var published = output.history().get(0);
    assertEquals("support-dlq", published.topic());
    assertEquals(
        "sha256:7e9d14274c25fe070ce7f8f22a826d69e6285baf36c0741fdfda2771f5ea3c63",
        published.key());
    assertEquals(Instant.parse("2026-10-02T13:45:12.345Z").toEpochMilli(), published.timestamp());
    JsonNode deadLetter = JSON.readTree(published.value());
    assertEquals("customer-42", deadLetter.at("/source/key").asText());
    assertEquals("hidden", deadLetter.at("/input/secret").asText());
    assertEquals("STATE_BUILDING", deadLetter.at("/error/category").asText());
    assertEquals("MISSING_TEMPLATE_VALUE", deadLetter.at("/error/code").asText());
    assertEquals("Required template reference is missing", deadLetter.at("/error/message").asText());
    assertEquals(0, deadLetter.at("/evaluation/attempt_count").asInt());
    assertEquals("unresolved:jev-latest", deadLetter.at("/evaluation/model/resolved").asText());
    assertEquals(
        DeterministicIds.evaluationId(
            published.key(),
            deadLetter.at("/evaluation/question_set/hash").asText(),
            deadLetter.at("/evaluation/state_policy/hash").asText(),
            "unresolved:jev-latest"),
        deadLetter.at("/evaluation/id").asText());
    assertFalse(deadLetter.at("/evaluation").has("state"));
    assertFalse(deadLetter.has("jev"));
    assertFalse(published.value().contains("Route it"));
    assertFalse(published.value().contains("test-key"));
    task.stop();
  }

  @Test
  void failedEvaluationUsesPinnedModelIdentityAndOnlyAliasesUseAnUnresolvedSentinel()
      throws Exception {
    SinkRecord missing = sourceRecord(null, "customer-42", null, Map.of("secret", "hidden"));

    MockProducer<String, String> aliasOutput = output();
    JevSinkTask aliasTask = new JevSinkTask(ignored -> aliasOutput);
    aliasTask.start(requiredTemplateConfig());
    aliasTask.put(List.of(missing));
    JsonNode aliasFailure = JSON.readTree(aliasOutput.history().get(0).value());
    assertEquals("unresolved:jev-latest", aliasFailure.at("/evaluation/model/resolved").asText());
    aliasTask.stop();

    Map<String, String> pinned = new java.util.HashMap<>(requiredTemplateConfig());
    pinned.put("jev.model", "jev-1.13.0");
    MockProducer<String, String> pinnedOutput = output();
    JevSinkTask pinnedTask = new JevSinkTask(ignored -> pinnedOutput);
    pinnedTask.start(pinned);
    pinnedTask.put(List.of(missing));
    JsonNode pinnedFailure = JSON.readTree(pinnedOutput.history().get(0).value());
    assertEquals("jev-1.13.0", pinnedFailure.at("/evaluation/model/requested").asText());
    assertEquals("jev-1.13.0", pinnedFailure.at("/evaluation/model/resolved").asText());
    assertEquals(
        DeterministicIds.evaluationId(
            pinnedFailure.at("/source/id").asText(),
            pinnedFailure.at("/evaluation/question_set/hash").asText(),
            pinnedFailure.at("/evaluation/state_policy/hash").asText(),
            "jev-1.13.0"),
        pinnedFailure.at("/evaluation/id").asText());
    assertEquals(
        "jev-1.13.0",
        utf8(pinnedOutput.history().get(0).headers().lastHeader("kafka-jev-resolved-model").value()));
    pinnedTask.stop();
  }

  @Test
  void tombstonesCanBeIgnoredDeadLetteredOrMadeTaskFatal() throws Exception {
    fakeJev = fakeJev(new AtomicReference<>());
    SinkRecord tombstone = sourceRecord(null, "customer-42", null, null);

    MockProducer<String, String> ignoredOutput = output();
    JevSinkTask ignoringTask = new JevSinkTask(ignored -> ignoredOutput);
    ignoringTask.start(config());
    ignoringTask.put(List.of(tombstone));
    assertEquals(0, ignoredOutput.history().size());
    ignoringTask.stop();

    Map<String, String> dlqConfig = new java.util.HashMap<>(config());
    dlqConfig.put("behavior.on.null.values", "DLQ");
    MockProducer<String, String> dlqOutput = output();
    JevSinkTask dlqTask = new JevSinkTask(ignored -> dlqOutput);
    dlqTask.start(dlqConfig);
    dlqTask.put(List.of(tombstone));
    assertEquals("support-dlq", dlqOutput.history().get(0).topic());
    JsonNode deadLetter = JSON.readTree(dlqOutput.history().get(0).value());
    assertEquals("TOMBSTONE", deadLetter.at("/error/code").asText());
    assertEquals(0, deadLetter.at("/evaluation/attempt_count").asInt());
    dlqTask.stop();

    Map<String, String> failConfig = new java.util.HashMap<>(config());
    failConfig.put("behavior.on.null.values", "FAIL");
    MockProducer<String, String> failedOutput = output();
    JevSinkTask failingTask = new JevSinkTask(ignored -> failedOutput);
    failingTask.start(failConfig);
    ConnectException failure =
        assertThrows(ConnectException.class, () -> failingTask.put(List.of(tombstone)));
    assertEquals("Tombstone Source Record configured to fail the task", failure.getMessage());
    assertEquals(0, failedOutput.history().size());
    failingTask.stop();
  }

  @Test
  void rejectsMalformedOrUnsupportedTemplatesAtStartup() throws Exception {
    fakeJev = fakeJev(new AtomicReference<>());
    List<String> invalidTemplates =
        List.of(
            "${unknown:anything}",
            "${metadata:unknown}",
            "${value:not-a-pointer}",
            "${value:/bad~2escape}",
            "${value:/unterminated",
            "bad\\q",
            "bad\\}");

    for (String template : invalidTemplates) {
      Map<String, String> invalid = new java.util.HashMap<>(config());
      invalid.put("state.mode", "TEMPLATE");
      invalid.put("state.template", template);
      assertThrows(
          ConfigException.class,
          () -> new JevSinkTask(ignored -> output()).start(invalid),
          template);
    }

    Map<String, String> missing = new java.util.HashMap<>(config());
    missing.put("state.mode", "TEMPLATE");
    assertThrows(ConfigException.class, () -> new JevSinkTask(ignored -> output()).start(missing));
  }

  private void assertFullValueState(
      JevSinkTask task,
      AtomicReference<JsonNode> receivedRequest,
      Schema schema,
      Object value,
      String expected) {
    task.put(List.of(sourceRecord(null, null, schema, value)));
    assertEquals(expected, receivedRequest.get().path("state").asText());
  }

  @Test
  void deadLettersUnsupportedOrInvalidRawBytesAndAcceptsValidUtf8() throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    MockProducer<String, String> rejectedOutput = output();
    JevSinkTask rejectingTask = new JevSinkTask(ignored -> rejectedOutput);
    rejectingTask.start(config());

    rejectingTask.put(
        List.of(
            sourceRecord(
                null, null, Schema.BYTES_SCHEMA, "Olá".getBytes(StandardCharsets.UTF_8))));
    assertEquals(
        "RAW_BYTES_DISABLED",
        JSON.readTree(rejectedOutput.history().get(0).value()).at("/error/code").asText());
    rejectingTask.stop();

    MockProducer<String, String> acceptedOutput = output();
    JevSinkTask acceptingTask = new JevSinkTask(ignored -> acceptedOutput);
    Map<String, String> enabled = new java.util.HashMap<>(config());
    enabled.put("state.raw_bytes.encoding", "UTF-8");
    acceptingTask.start(enabled);
    acceptingTask.put(
        List.of(
            sourceRecord(
                null, null, Schema.BYTES_SCHEMA, "Olá".getBytes(StandardCharsets.UTF_8))));

    assertEquals("Olá", receivedRequest.get().path("state").asText());
    JsonNode enriched = JSON.readTree(acceptedOutput.history().get(0).value());
    assertEquals(
        JSON.readTree("{\"$type\":\"bytes\",\"base64\":\"T2zDoQ==\"}"),
        enriched.path("input"));

    acceptingTask.put(
        List.of(
            sourceRecord(
                null,
                null,
                Schema.BYTES_SCHEMA,
                ByteBuffer.wrap("buffer".getBytes(StandardCharsets.UTF_8)))));
    assertEquals("buffer", receivedRequest.get().path("state").asText());

    acceptingTask.put(
        List.of(
            sourceRecord(null, null, Schema.BYTES_SCHEMA, new byte[] {(byte) 0xc3, 0x28})));
    JsonNode invalidUtf8 = JSON.readTree(acceptedOutput.history().get(2).value());
    assertEquals("support-dlq", acceptedOutput.history().get(2).topic());
    assertEquals("INVALID_UTF8", invalidUtf8.at("/error/code").asText());
    assertFalse(invalidUtf8.at("/evaluation").has("state"));
    acceptingTask.stop();
  }

  @Test
  void templateWholeValueAppliesTheRawBytesPolicyAndStrictUtf8Decoding() throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    Map<String, String> template = new java.util.HashMap<>(config());
    template.put("state.mode", "TEMPLATE");
    template.put("state.template", "payload=${value}");

    MockProducer<String, String> disabledOutput = output();
    JevSinkTask disabled = new JevSinkTask(ignored -> disabledOutput);
    disabled.start(template);
    disabled.put(
        List.of(
            sourceRecord(
                null, null, Schema.BYTES_SCHEMA, "private".getBytes(StandardCharsets.UTF_8))));
    assertEquals(
        "RAW_BYTES_DISABLED",
        JSON.readTree(disabledOutput.history().get(0).value()).at("/error/code").asText());
    assertEquals(null, receivedRequest.get());
    disabled.stop();

    template.put("state.raw_bytes.encoding", "UTF-8");
    MockProducer<String, String> enabledOutput = output();
    JevSinkTask enabled = new JevSinkTask(ignored -> enabledOutput);
    enabled.start(template);
    enabled.put(
        List.of(
            sourceRecord(
                null, null, Schema.BYTES_SCHEMA, "Olá".getBytes(StandardCharsets.UTF_8))));
    assertEquals("payload=Olá", receivedRequest.get().path("state").asText());

    enabled.put(
        List.of(
            sourceRecord(null, null, Schema.BYTES_SCHEMA, new byte[] {(byte) 0xc3, 0x28})));
    assertEquals(
        "INVALID_UTF8",
        JSON.readTree(enabledOutput.history().get(1).value()).at("/error/code").asText());
    enabled.stop();
  }

  @Test
  void deadLettersEvaluationStateThatExceedsTheConfiguredUtf8ByteLimit() throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    Map<String, String> limited = new java.util.HashMap<>(config());
    limited.put("state.mode", "TEMPLATE");
    limited.put("state.template", "generated=${value}");
    limited.put("state.max.bytes", "8");
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(limited);

    task.put(List.of(sourceRecord(null, "customer-42", null, "Olá")));

    assertEquals(null, receivedRequest.get());
    JsonNode deadLetter = JSON.readTree(output.history().get(0).value());
    assertEquals("STATE_TOO_LARGE", deadLetter.at("/error/code").asText());
    assertEquals(
        DeterministicIds.evaluationStateHash("generated=Olá"),
        deadLetter.at("/evaluation/state/hash").asText());
    assertFalse(output.history().get(0).value().contains("generated=Olá"));
    task.stop();
  }

  @Test
  void waitsForDeadLetterPublicationAndFailsTheTaskWhenItFails() throws Exception {
    fakeJev = fakeJev(new AtomicReference<>());
    Map<String, String> required = new java.util.HashMap<>(config());
    required.put("state.mode", "TEMPLATE");
    required.put("state.template", "approved=${value:/approved}");
    MockProducer<String, String> output = output();
    output.sendException = new RuntimeException("broker unavailable");
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(required);

    ConnectException failure =
        assertThrows(
            ConnectException.class,
            () -> task.put(List.of(sourceRecord(null, null, null, Map.of("safe", true)))));

    assertEquals("Failed to publish Dead-Letter Record", failure.getMessage());
    task.stop();
  }

  @Test
  void pluginOwnedProducerCannotBeConfiguredToWeakenPublicationGuarantees() {
    AtomicReference<Map<String, Object>> producerProperties = new AtomicReference<>();
    MockProducer<String, String> output = output();
    JevSinkTask task =
        new JevSinkTask(
            properties -> {
              producerProperties.set(Map.copyOf(properties));
              return output;
            });
    Map<String, String> attemptedOverrides =
        new java.util.HashMap<>(config("http://127.0.0.1:1/evaluate"));
    attemptedOverrides.put("producer.override.enable.idempotence", "false");
    attemptedOverrides.put("producer.override.acks", "0");
    attemptedOverrides.put("producer.override.key.serializer", "unsafe.KeySerializer");
    attemptedOverrides.put("producer.override.value.serializer", "unsafe.ValueSerializer");
    attemptedOverrides.put("producer.override.allow.auto.create.topics", "true");
    attemptedOverrides.put("producer.override.security.protocol", "PLAINTEXT");

    task.start(attemptedOverrides);

    Map<String, Object> actual = producerProperties.get();
    assertEquals(true, actual.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    assertEquals("all", actual.get(ProducerConfig.ACKS_CONFIG));
    assertEquals(StringSerializer.class.getName(), actual.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    assertEquals(
        StringSerializer.class.getName(), actual.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    assertEquals(false, actual.get("allow.auto.create.topics"));
    assertFalse(actual.keySet().stream().anyMatch(name -> name.startsWith("producer.override.")));
    task.stop();
  }

  @Test
  void doesNotCompleteTheDeliveredBatchUntilItsPublicationIsAcknowledged() throws Exception {
    ControllableProducer output = new ControllableProducer();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(requiredTemplateConfig());
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      Future<?> deliveredBatch =
          worker.submit(
              () -> task.put(List.of(sourceRecord(null, "customer-42", null, "safe value"))));

      assertTrue(output.publicationAttempted.await(2, TimeUnit.SECONDS));
      assertFalse(deliveredBatch.isDone());

      assertTrue(output.completeNext());
      deliveredBatch.get(2, TimeUnit.SECONDS);
    } finally {
      worker.shutdownNow();
      task.stop();
    }
  }

  @Test
  void publicationFailureLeavesTheDeliveredBatchIneligibleForOffsetCommit() throws Exception {
    MockProducer<String, String> output = output();
    output.sendException = new RuntimeException("broker unavailable");
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(requiredTemplateConfig());
    TopicPartition input = new TopicPartition("support-input", 2);
    Map<TopicPartition, OffsetAndMetadata> deliveredOffsets =
        Map.of(input, new OffsetAndMetadata(185));

    assertThrows(
        ConnectException.class,
        () -> task.put(List.of(sourceRecord(null, "customer-42", null, "safe value"))));

    assertEquals(Map.of(), task.preCommit(deliveredOffsets));

    output.sendException = null;
    task.put(List.of(sourceRecord(null, "customer-42", null, "safe value")));
    assertEquals(deliveredOffsets, task.preCommit(deliveredOffsets));
    task.stop();
  }

  @Test
  void enrichedPublicationFailureAlsoLeavesTheDeliveredBatchIneligibleForOffsetCommit()
      throws Exception {
    fakeJev = fakeJev(new AtomicReference<>());
    MockProducer<String, String> output = output();
    output.sendException = new RuntimeException("broker unavailable");
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(config());
    Map<TopicPartition, OffsetAndMetadata> deliveredOffsets =
        Map.of(new TopicPartition("support-input", 2), new OffsetAndMetadata(185));

    ConnectException failure =
        assertThrows(
            ConnectException.class,
            () -> task.put(List.of(sourceRecord(null, "customer-42", null, "safe value"))));

    assertEquals("Failed to publish Enriched Record", failure.getMessage());
    assertEquals(Map.of(), task.preCommit(deliveredOffsets));
    task.stop();
  }

  @Test
  void neverExceedsTheConfiguredMaximumOfInFlightJevRequests() throws Exception {
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger maximumInFlight = new AtomicInteger();
    CountDownLatch twoRequestsStarted = new CountDownLatch(2);
    CountDownLatch releaseRequests = new CountDownLatch(1);
    fakeJev = concurrentFakeJev(inFlight, maximumInFlight, twoRequestsStarted, releaseRequests);
    Map<String, String> bounded = new java.util.HashMap<>(config());
    bounded.put("jev.max.in.flight", "2");
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(bounded);
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      Future<?> deliveredBatch =
          worker.submit(
              () ->
                  task.put(
                      List.of(
                          sourceRecord(184, "first"),
                          sourceRecord(185, "second"),
                          sourceRecord(186, "third"),
                          sourceRecord(187, "fourth"))));

      assertTrue(twoRequestsStarted.await(2, TimeUnit.SECONDS));
      assertEquals(2, maximumInFlight.get());
      assertFalse(deliveredBatch.isDone());

      releaseRequests.countDown();
      deliveredBatch.get(2, TimeUnit.SECONDS);
      assertEquals(4, output.history().size());
      assertEquals(2, maximumInFlight.get());
    } finally {
      releaseRequests.countDown();
      worker.shutdownNow();
      task.stop();
    }
  }

  @Test
  void mixedSuccessfulAndPermanentFailureRecordsPublishTheirOwnOutcomes() throws Exception {
    fakeJev = stateAwareFakeJev();
    Map<String, String> concurrent = new java.util.HashMap<>(config());
    concurrent.put("jev.max.in.flight", "2");
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(concurrent);

    task.put(
        List.of(
            sourceRecord(184, "first success"),
            sourceRecord(185, "reject this record"),
            sourceRecord(186, "second success")));

    assertEquals(
        2, output.history().stream().filter(record -> "support-output".equals(record.topic())).count());
    assertEquals(
        1, output.history().stream().filter(record -> "support-dlq".equals(record.topic())).count());
    ProducerRecord<String, String> deadLetter =
        output.history().stream()
            .filter(record -> "support-dlq".equals(record.topic()))
            .findFirst()
            .orElseThrow();
    assertEquals("RECORD_TOO_LARGE", JSON.readTree(deadLetter.value()).at("/error/code").asText());
    task.stop();
  }

  @Test
  void doesNotCompleteAMultiRecordBatchUntilEveryPublicationIsAcknowledged() throws Exception {
    fakeJev = fakeJev(new AtomicReference<>());
    ControllableProducer output = new ControllableProducer(2);
    Map<String, String> concurrent = new java.util.HashMap<>(config());
    concurrent.put("jev.max.in.flight", "2");
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(concurrent);
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      Future<?> deliveredBatch =
          worker.submit(
              () -> task.put(List.of(sourceRecord(184, "first"), sourceRecord(185, "second"))));

      assertTrue(output.publicationAttempted.await(2, TimeUnit.SECONDS));
      assertFalse(deliveredBatch.isDone());

      assertTrue(output.completeNext());
      assertFalse(deliveredBatch.isDone());
      assertTrue(output.completeNext());
      deliveredBatch.get(2, TimeUnit.SECONDS);
    } finally {
      worker.shutdownNow();
      task.stop();
    }
  }

  @Test
  void exhaustedTransientFailureKeepsTheMixedBatchIneligibleForCommit() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    fakeJev =
        fakeJevResponses(
            attempts,
            List.of(
                new FakeResponse(503, "private outage", null),
                new FakeResponse(200, "{\"model\":\"jev-1.13.0\"}", null)));
    Map<String, String> concurrent = new java.util.HashMap<>(config());
    concurrent.put("jev.max.in.flight", "2");
    concurrent.put("jev.retry.max.attempts", "1");
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(concurrent);
    Map<TopicPartition, OffsetAndMetadata> deliveredOffsets =
        Map.of(new TopicPartition("support-input", 2), new OffsetAndMetadata(186));

    assertThrows(
        ConnectException.class,
        () -> task.put(List.of(sourceRecord(184, "one"), sourceRecord(185, "two"))));

    assertEquals(2, attempts.get());
    assertEquals(1, output.history().size());
    assertEquals("support-output", output.history().get(0).topic());
    assertEquals(Map.of(), task.preCommit(deliveredOffsets));
    task.stop();
  }

  @Test
  void replayAndEquivalentConfigurationKeepIdentityWhileEffectiveChangesCreateANewEvaluation()
      throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    SinkRecord source = sourceRecord(null, "customer-42", null, "same source");

    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(config());
    task.put(List.of(source));
    task.put(List.of(source));
    JsonNode first = JSON.readTree(output.history().get(0).value());
    JsonNode replay = JSON.readTree(output.history().get(1).value());
    assertEquals(first.at("/source/id"), replay.at("/source/id"));
    assertEquals(first.at("/evaluation/id"), replay.at("/evaluation/id"));

    Map<String, String> reorderedQuestions = new java.util.HashMap<>(config());
    reorderedQuestions.put(
        "jev.questions",
        "{\"department\":{\"instructions\":\"Route it\",\"type\":\"choice\"}}");
    MockProducer<String, String> equivalentOutput = output();
    JevSinkTask equivalentTask = new JevSinkTask(ignored -> equivalentOutput);
    equivalentTask.start(reorderedQuestions);
    equivalentTask.put(List.of(source));
    JsonNode equivalent = JSON.readTree(equivalentOutput.history().get(0).value());
    assertEquals(first.at("/evaluation/question_set/hash"), equivalent.at("/evaluation/question_set/hash"));
    assertEquals(first.at("/evaluation/id"), equivalent.at("/evaluation/id"));

    Map<String, String> changedQuestions = new java.util.HashMap<>(config());
    changedQuestions.put(
        "jev.questions",
        "{\"department\":{\"type\":\"choice\",\"instructions\":\"Escalate it\"}}");
    JsonNode changedQuestionEvaluation = evaluate(source, changedQuestions);
    assertNotEquals(
        first.at("/evaluation/question_set/hash"),
        changedQuestionEvaluation.at("/evaluation/question_set/hash"));
    assertNotEquals(first.at("/evaluation/id"), changedQuestionEvaluation.at("/evaluation/id"));

    Map<String, String> changedPolicy = new java.util.HashMap<>(config());
    changedPolicy.put("state.raw_bytes.encoding", "UTF-8");
    JsonNode changedPolicyEvaluation = evaluate(source, changedPolicy);
    assertNotEquals(
        first.at("/evaluation/state_policy/hash"),
        changedPolicyEvaluation.at("/evaluation/state_policy/hash"));
    assertNotEquals(first.at("/evaluation/id"), changedPolicyEvaluation.at("/evaluation/id"));

    resolvedModel.set("jev-1.14.0");
    JsonNode changedModelEvaluation = evaluate(source, config());
    assertEquals("jev-1.14.0", changedModelEvaluation.at("/evaluation/model/resolved").asText());
    assertNotEquals(first.at("/evaluation/id"), changedModelEvaluation.at("/evaluation/id"));

    task.stop();
    equivalentTask.stop();
  }

  @Test
  void rejectsInferenceResultsWithoutAUsableResolvedModel() throws Exception {
    AtomicReference<String> responseBody =
        new AtomicReference<>("{\"model\":\" jev-1.13.0 \"}");
    fakeJev = fakeJevResponse(responseBody);
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(config());

    for (String invalidResponse :
        List.of(
            "[]",
            "\"not an object\"",
            "{}",
            "{\"model\":null}",
            "{\"model\":13}",
            "{\"model\":\"\"}",
            "{\"model\":\" jev-1.13.0 \"}")) {
      responseBody.set(invalidResponse);
      ConnectException failure =
          assertThrows(
              ConnectException.class,
              () -> task.put(List.of(sourceRecord(null, "customer-42", null, "same source"))));
      assertEquals("Jev response must be a JSON object with a usable model", failure.getMessage());
    }
    assertEquals(0, output.history().size());
    task.stop();
  }

  @Test
  void pinnedAndAliasRequestsRetainDistinctProvenanceButShareResolvedModelIdentity()
      throws Exception {
    String response =
        "{\"model\":\"jev-1.13.0\",\"answers\":{\"department\":\"technical\"},"
            + "\"usage\":{\"input_tokens\":17},\"provider_metadata\":{\"trace\":\"opaque\"},"
            + "\"future_field\":[false,0,\"\"]}";
    fakeJev = fakeJevResponse(new AtomicReference<>(response));
    SinkRecord source = sourceRecord(null, "customer-42", null, "same source");

    JsonNode aliasEvaluation = evaluate(source, config());
    Map<String, String> pinnedConfig = new java.util.HashMap<>(config());
    pinnedConfig.put("jev.model", "jev-1.13.0");
    JsonNode pinnedEvaluation = evaluate(source, pinnedConfig);

    assertEquals("jev-latest", aliasEvaluation.at("/evaluation/model/requested").asText());
    assertEquals("jev-1.13.0", pinnedEvaluation.at("/evaluation/model/requested").asText());
    assertEquals("jev-1.13.0", aliasEvaluation.at("/evaluation/model/resolved").asText());
    assertEquals("jev-1.13.0", pinnedEvaluation.at("/evaluation/model/resolved").asText());
    assertEquals(
        "sha256:dcc2ee43358f5b0068a0ad5d2e499b306524c96d74c167f60def5dce019e3fa9",
        aliasEvaluation.at("/evaluation/id").asText());
    assertEquals(aliasEvaluation.at("/evaluation/id"), pinnedEvaluation.at("/evaluation/id"));
    assertEquals(JSON.readTree(response), aliasEvaluation.path("jev"));
    assertEquals(JSON.readTree(response), pinnedEvaluation.path("jev"));
  }

  @Test
  void retriesRateLimitingAndPublishesOnlyAggregateAttemptEvidence() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    fakeJev =
        fakeJevResponses(
            attempts,
            List.of(
                new FakeResponse(429, "private throttling details", "120"),
                new FakeResponse(503, "temporary outage details", null),
                new FakeResponse(
                    200,
                    "{\"model\":\"jev-1.13.0\",\"answers\":{},\"usage\":{}}",
                    null)));
    Map<String, String> retrying = new java.util.HashMap<>(config());
    retrying.put("jev.retry.max.attempts", "3");
    retrying.put("jev.retry.initial.backoff.ms", "20");
    retrying.put("jev.retry.max.retry_after.ms", "100");
    AtomicLong monotonicNanos = new AtomicLong();
    List<Long> delays = new ArrayList<>();
    MockProducer<String, String> output = output();
    JevSinkTask task =
        new JevSinkTask(
            ignored -> output,
            delay -> {
              delays.add(delay);
              monotonicNanos.addAndGet(delay * 1_000_000L);
            },
            () -> 1.25,
            monotonicNanos::get);
    task.start(retrying);

    task.put(List.of(sourceRecord(null, "customer-42", null, "same source")));

    assertEquals(3, attempts.get());
    assertEquals(List.of(100L, 50L), delays);
    JsonNode enriched = JSON.readTree(output.history().get(0).value());
    assertEquals(3, enriched.at("/evaluation/attempt_count").asInt());
    assertEquals(150, enriched.at("/evaluation/duration_ms").asLong());
    assertFalse(output.history().get(0).value().contains("private throttling details"));
    assertFalse(enriched.at("/evaluation").has("attempts"));
    task.stop();
  }

  @Test
  void retriesEveryDocumentedTransientHttpStatus() throws Exception {
    for (int status : List.of(408, 429, 500, 502, 529, 599)) {
      AtomicInteger attempts = new AtomicInteger();
      fakeJev =
          fakeJevResponses(
              attempts,
              List.of(
                  new FakeResponse(status, "remote details must stay private", null),
                  new FakeResponse(200, "{\"model\":\"jev-1.13.0\"}", null)));
      Map<String, String> retrying = new java.util.HashMap<>(config());
      retrying.put("jev.retry.max.attempts", "2");
      retrying.put("jev.retry.initial.backoff.ms", "0");
      MockProducer<String, String> output = output();
      JevSinkTask task = new JevSinkTask(ignored -> output);
      task.start(retrying);

      task.put(List.of(sourceRecord(null, null, null, "safe")));

      assertEquals(2, attempts.get(), "HTTP " + status);
      assertEquals(
          2,
          JSON.readTree(output.history().get(0).value())
              .at("/evaluation/attempt_count")
              .asInt());
      assertFalse(output.history().get(0).value().contains("remote details"));
      task.stop();
      fakeJev.stop(0);
      fakeJev = null;
    }
  }

  @Test
  void failsImmediatelyForConfigurationCredentialProtocolAndUnknownStatuses() throws Exception {
    for (int status : List.of(400, 401, 403, 404, 422, 201, 418)) {
      AtomicInteger attempts = new AtomicInteger();
      fakeJev =
          fakeJevResponses(
              attempts,
              List.of(new FakeResponse(status, "remote secret error body", null)));
      MockProducer<String, String> output = output();
      JevSinkTask task = new JevSinkTask(ignored -> output);
      task.start(config());

      ConnectException failure =
          assertThrows(
              ConnectException.class,
              () -> task.put(List.of(sourceRecord(null, null, null, "safe"))),
              "HTTP " + status);

      assertEquals(1, attempts.get(), "HTTP " + status);
      assertFalse(failure.getMessage().contains("remote secret"));
      assertEquals(0, output.history().size());
      task.stop();
      fakeJev.stop(0);
      fakeJev = null;
    }
  }

  @Test
  void malformedJsonFailsSafelyWithoutPublishingRemoteContent() throws Exception {
    fakeJev = fakeJevResponse(new AtomicReference<>("{malformed private response"));
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(config());

    ConnectException failure =
        assertThrows(
            ConnectException.class,
            () -> task.put(List.of(sourceRecord(null, null, null, "safe"))));

    assertEquals("Jev response is not valid JSON", failure.getMessage());
    assertFalse(failure.toString().contains("private response"));
    assertEquals(0, output.history().size());
    task.stop();
  }

  @Test
  void exhaustedTransientFailuresFailTheTaskByDefaultWithoutPublishing() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    fakeJev =
        fakeJevResponses(
            attempts, List.of(new FakeResponse(503, "private outage diagnostics", null)));
    Map<String, String> exhausted = new java.util.HashMap<>(config());
    exhausted.put("jev.retry.max.attempts", "2");
    exhausted.put("jev.retry.initial.backoff.ms", "0");
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(exhausted);

    ConnectException failure =
        assertThrows(
            ConnectException.class,
            () -> task.put(List.of(sourceRecord(null, null, null, "safe source value"))));

    assertEquals("Jev Evaluation exhausted 2 transient attempts", failure.getMessage());
    assertEquals(2, attempts.get());
    assertEquals(0, output.history().size());
    assertFalse(failure.toString().contains("private outage"));
    task.stop();
  }

  @Test
  void explicitDlqPolicyPublishesSanitizedAggregateTransientExhaustion() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    fakeJev =
        fakeJevResponses(
            attempts, List.of(new FakeResponse(503, "private outage diagnostics", null)));
    Map<String, String> exhausted = new java.util.HashMap<>(config());
    exhausted.put("jev.retry.max.attempts", "3");
    exhausted.put("jev.retry.initial.backoff.ms", "10");
    exhausted.put("errors.transient.exhausted", "DLQ");
    AtomicLong monotonicNanos = new AtomicLong();
    MockProducer<String, String> output = output();
    JevSinkTask task =
        new JevSinkTask(
            ignored -> output,
            delay -> monotonicNanos.addAndGet(delay * 1_000_000L),
            () -> 1.0,
            monotonicNanos::get);
    task.start(exhausted);

    task.put(List.of(sourceRecord(null, null, null, "safe source value")));

    assertEquals(3, attempts.get());
    assertEquals(1, output.history().size());
    assertEquals("support-dlq", output.history().get(0).topic());
    JsonNode deadLetter = JSON.readTree(output.history().get(0).value());
    assertEquals("JEV_SERVICE", deadLetter.at("/error/category").asText());
    assertEquals("TRANSIENT_EXHAUSTED", deadLetter.at("/error/code").asText());
    assertEquals(3, deadLetter.at("/evaluation/attempt_count").asInt());
    assertEquals(30, deadLetter.at("/evaluation/duration_ms").asLong());
    assertEquals(
        DeterministicIds.evaluationStateHash("safe source value"),
        deadLetter.at("/evaluation/state/hash").asText());
    assertFalse(output.history().get(0).value().contains("private outage"));
    assertFalse(deadLetter.at("/evaluation").has("attempts"));
    task.stop();
  }

  @Test
  void jevRecordSizeRejectionIsAPermanentRecordFailure() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    fakeJev =
        fakeJevResponses(
            attempts, List.of(new FakeResponse(413, "raw service size details", null)));
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(config());

    task.put(List.of(sourceRecord(null, null, null, "safe source value")));

    assertEquals(1, attempts.get());
    JsonNode deadLetter = JSON.readTree(output.history().get(0).value());
    assertEquals("support-dlq", output.history().get(0).topic());
    assertEquals("RECORD_TOO_LARGE", deadLetter.at("/error/code").asText());
    assertEquals(1, deadLetter.at("/evaluation/attempt_count").asInt());
    assertEquals(
        DeterministicIds.evaluationStateHash("safe source value"),
        deadLetter.at("/evaluation/state/hash").asText());
    assertFalse(output.history().get(0).value().contains("raw service size details"));
    task.stop();
  }

  @Test
  void requestTimeoutsUseTheConfiguredTotalAttemptLimit() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    fakeJev = slowFakeJev(attempts);
    Map<String, String> timingOut = new java.util.HashMap<>(config());
    timingOut.put("jev.request.timeout.ms", "25");
    timingOut.put("jev.retry.max.attempts", "2");
    timingOut.put("jev.retry.initial.backoff.ms", "0");
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(timingOut);

    ConnectException failure =
        assertThrows(
            ConnectException.class,
            () -> task.put(List.of(sourceRecord(null, null, null, "safe"))));

    assertEquals("Jev Evaluation exhausted 2 transient attempts", failure.getMessage());
    assertEquals(2, attempts.get());
    assertEquals(0, output.history().size());
    task.stop();
  }

  @Test
  void connectionFailuresAreTransientAndExhaustTheConfiguredAttemptLimit() throws Exception {
    fakeJev = fakeJev(new AtomicReference<>());
    Map<String, String> unavailable = new java.util.HashMap<>(config());
    fakeJev.stop(0);
    int unusedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      unusedPort = socket.getLocalPort();
    }
    unavailable.put("jev.endpoint", "http://127.0.0.1:" + unusedPort + "/evaluate");
    unavailable.put("jev.connect.timeout.ms", "25");
    unavailable.put("jev.retry.max.attempts", "2");
    unavailable.put("jev.retry.initial.backoff.ms", "0");
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(unavailable);

    ConnectException failure =
        assertThrows(
            ConnectException.class,
            () -> task.put(List.of(sourceRecord(null, null, null, "safe"))));

    assertEquals("Jev Evaluation exhausted 2 transient attempts", failure.getMessage());
    assertEquals(0, output.history().size());
    task.stop();
  }

  private JsonNode evaluate(SinkRecord source, Map<String, String> connectorConfig) throws Exception {
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    task.start(connectorConfig);
    task.put(List.of(source));
    task.stop();
    return JSON.readTree(output.history().get(0).value());
  }

  private SinkRecord sourceRecord(
      Schema keySchema, Object key, Schema valueSchema, Object value) {
    return new SinkRecord(
        "support-input",
        2,
        keySchema,
        key,
        valueSchema,
        value,
        184,
        Instant.parse("2026-10-02T13:45:12.345Z").toEpochMilli(),
        TimestampType.CREATE_TIME);
  }

  private SinkRecord sourceRecord(long offset, Object value) {
    return new SinkRecord(
        "support-input",
        2,
        null,
        "customer-" + offset,
        null,
        value,
        offset,
        Instant.parse("2026-10-02T13:45:12.345Z").toEpochMilli(),
        TimestampType.CREATE_TIME);
  }

  private MockProducer<String, String> output() {
    return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
  }

  private static String utf8(byte[] value) {
    return new String(value, StandardCharsets.UTF_8);
  }

  private static final class ControllableProducer extends MockProducer<String, String> {
    private final CountDownLatch publicationAttempted;

    private ControllableProducer() {
      this(1);
    }

    private ControllableProducer(int expectedPublications) {
      super(false, null, new StringSerializer(), new StringSerializer());
      publicationAttempted = new CountDownLatch(expectedPublications);
    }

    @Override
    public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record) {
      Future<RecordMetadata> publication = super.send(record);
      publicationAttempted.countDown();
      return publication;
    }
  }

  private Map<String, String> config() {
    return config(endpoint());
  }

  private Map<String, String> config(String endpoint) {
    return Map.ofEntries(
        Map.entry("name", "support-evaluator"),
        Map.entry("topics", "support-input"),
        Map.entry("output.topic", "support-output"),
        Map.entry("errors.deadletter.topic", "support-dlq"),
        Map.entry("question.set.id", "support-routing/v1"),
        Map.entry("jev.api.key", "test-key"),
        Map.entry(
            "jev.questions",
            "{\"department\":{\"type\":\"choice\",\"instructions\":\"Route it\"}}"),
        Map.entry("state.mode", "FULL_VALUE"),
        Map.entry("jev.model", "jev-latest"),
        Map.entry("jev.endpoint", endpoint),
        Map.entry("jev.allow.insecure.http", "true"),
        Map.entry("output.bootstrap.servers", "unused:9092"));
  }

  private Map<String, String> requiredTemplateConfig() {
    Map<String, String> required =
        new java.util.HashMap<>(config("http://127.0.0.1:1/evaluate"));
    required.put("state.mode", "TEMPLATE");
    required.put("state.template", "approved=${value:/approved}");
    return required;
  }

  private HttpServer fakeJev(AtomicReference<JsonNode> receivedRequest) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/evaluate",
        exchange -> {
          receivedRequest.set(JSON.readTree(exchange.getRequestBody()));
          byte[] response =
              ("{\"model\":\""
                      + resolvedModel.get()
                      + "\",\"answers\":{\"department\":\"technical\"},\"usage\":{\"provider\":\"fake\"}}")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    return server;
  }

  private HttpServer fakeJevResponse(AtomicReference<String> responseBody) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/evaluate",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          byte[] response = responseBody.get().getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    return server;
  }

  private HttpServer fakeJevResponses(AtomicInteger attempts, List<FakeResponse> responses)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/evaluate",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          int attempt = attempts.getAndIncrement();
          FakeResponse scripted = responses.get(Math.min(attempt, responses.size() - 1));
          byte[] response = scripted.body().getBytes(StandardCharsets.UTF_8);
          if (scripted.retryAfter() != null) {
            exchange.getResponseHeaders().set("Retry-After", scripted.retryAfter());
          }
          exchange.sendResponseHeaders(scripted.status(), response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    return server;
  }

  private HttpServer slowFakeJev(AtomicInteger attempts) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    fakeJevExecutor = Executors.newCachedThreadPool();
    server.setExecutor(fakeJevExecutor);
    server.createContext(
        "/evaluate",
        exchange -> {
          attempts.incrementAndGet();
          exchange.getRequestBody().readAllBytes();
          try {
            Thread.sleep(250);
            byte[] response = "{\"model\":\"jev-1.13.0\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
          } finally {
            exchange.close();
          }
        });
    server.start();
    return server;
  }

  private HttpServer concurrentFakeJev(
      AtomicInteger inFlight,
      AtomicInteger maximumInFlight,
      CountDownLatch requestsStarted,
      CountDownLatch releaseRequests)
      throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    fakeJevExecutor = Executors.newCachedThreadPool();
    server.setExecutor(fakeJevExecutor);
    server.createContext(
        "/evaluate",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          int currentInFlight = inFlight.incrementAndGet();
          maximumInFlight.accumulateAndGet(currentInFlight, Math::max);
          requestsStarted.countDown();
          try {
            releaseRequests.await(2, TimeUnit.SECONDS);
            byte[] response = "{\"model\":\"jev-1.13.0\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
          } finally {
            inFlight.decrementAndGet();
            exchange.close();
          }
        });
    server.start();
    return server;
  }

  private HttpServer stateAwareFakeJev() throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    fakeJevExecutor = Executors.newCachedThreadPool();
    server.setExecutor(fakeJevExecutor);
    server.createContext(
        "/evaluate",
        exchange -> {
          String state = JSON.readTree(exchange.getRequestBody()).path("state").asText();
          boolean rejected = state.contains("reject");
          byte[] response =
              (rejected ? "private size details" : "{\"model\":\"jev-1.13.0\"}")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(rejected ? 413 : 200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    return server;
  }

  private record FakeResponse(int status, String body, String retryAfter) {}

  private String endpoint() {
    return "http://127.0.0.1:" + fakeJev.getAddress().getPort() + "/evaluate";
  }
}
