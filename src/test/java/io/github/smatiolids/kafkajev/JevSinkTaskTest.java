package io.github.smatiolids.kafkajev;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JevSinkTaskTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  private HttpServer fakeJev;

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

  private HttpServer fakeJev(AtomicReference<JsonNode> receivedRequest) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/evaluate",
        exchange -> {
          receivedRequest.set(JSON.readTree(exchange.getRequestBody()));
          byte[] response =
              "{\"model\":\"jev-1.13.0\",\"answers\":{\"department\":\"technical\"},\"usage\":{\"provider\":\"fake\"}}"
                  .getBytes(StandardCharsets.UTF_8);
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
