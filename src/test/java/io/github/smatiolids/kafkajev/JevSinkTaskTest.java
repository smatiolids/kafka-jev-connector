package io.github.smatiolids.kafkajev;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JevSinkTaskTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  private HttpServer fakeJev;
  private final AtomicReference<String> resolvedModel = new AtomicReference<>("jev-1.13.0");

  @AfterEach
  void stopFakeJev() {
    if (fakeJev != null) {
      fakeJev.stop(0);
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
    JevSinkConnector connector = new JevSinkConnector();
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
  void rejectsRawBytesUnlessUtf8DecodingIsExplicitlyEnabled() throws Exception {
    AtomicReference<JsonNode> receivedRequest = new AtomicReference<>();
    fakeJev = fakeJev(receivedRequest);
    MockProducer<String, String> rejectedOutput = output();
    JevSinkTask rejectingTask = new JevSinkTask(ignored -> rejectedOutput);
    rejectingTask.start(config());

    ConnectException rejected =
        assertThrows(
            ConnectException.class,
            () ->
                rejectingTask.put(
                    List.of(
                        sourceRecord(
                            null,
                            null,
                            Schema.BYTES_SCHEMA,
                            "Olá".getBytes(StandardCharsets.UTF_8)))));
    assertEquals("Raw byte Evaluation State requires state.raw_bytes.encoding=UTF-8", rejected.getMessage());
    assertEquals(0, rejectedOutput.history().size());
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

    ConnectException invalidUtf8 =
        assertThrows(
            ConnectException.class,
            () ->
                acceptingTask.put(
                    List.of(
                        sourceRecord(null, null, Schema.BYTES_SCHEMA, new byte[] {(byte) 0xc3, 0x28}))));
    assertEquals("Raw byte Evaluation State is not valid UTF-8", invalidUtf8.getMessage());
    acceptingTask.stop();
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

  private MockProducer<String, String> output() {
    return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
  }

  private Map<String, String> config() {
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
        Map.entry("jev.endpoint", endpoint()),
        Map.entry("jev.allow.insecure.http", "true"),
        Map.entry("output.bootstrap.servers", "unused:9092"));
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

  private String endpoint() {
    return "http://127.0.0.1:" + fakeJev.getAddress().getPort() + "/evaluate";
  }
}
