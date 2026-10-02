package io.github.smatiolids.kafkajev;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.SimpleLogger;

class JevSecurityBoundaryTest {
  @Test
  void rejectsRedirectsWithoutSendingEvaluationStateToTheRedirectTarget() throws Exception {
    AtomicInteger redirectedRequests = new AtomicInteger();
    HttpServer target = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    target.createContext(
        "/capture",
        exchange -> {
          redirectedRequests.incrementAndGet();
          exchange.sendResponseHeaders(200, 0);
          exchange.close();
        });
    target.start();
    HttpServer redirector = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    redirector.createContext(
        "/evaluate",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          exchange.getResponseHeaders().set(
              "Location", "http://127.0.0.1:" + target.getAddress().getPort() + "/capture");
          exchange.sendResponseHeaders(307, -1);
          exchange.close();
        });
    redirector.start();
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output);
    try {
      task.start(localConfig("http://127.0.0.1:" + redirector.getAddress().getPort() + "/evaluate"));

      assertThrows(ConnectException.class, () -> task.put(List.of(source("generated-state-secret"))));

      assertEquals(0, redirectedRequests.get());
      assertEquals(0, output.history().size());
    } finally {
      task.stop();
      redirector.stop(0);
      target.stop(0);
    }
  }

  @Test
  void omitsCredentialsQuestionsStateAndRemoteBodiesFromDiagnosticsAndLogs() throws Exception {
    String remoteBody = "unsanitized-remote-body-secret";
    HttpServer jev = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    jev.createContext(
        "/evaluate",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          byte[] body = remoteBody.getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(500, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    jev.start();
    MockProducer<String, String> output = output();
    JevSinkTask task = new JevSinkTask(ignored -> output, millis -> {}, () -> 1.0, System::nanoTime);
    Map<String, String> config =
        localConfig("http://127.0.0.1:" + jev.getAddress().getPort() + "/evaluate");
    config.put("jev.api.key", "jev-credential-secret");
    config.put("jev.questions", "{\"private-question-secret\":{\"type\":\"boolean\"}}");
    config.put("jev.retry.max.attempts", "1");
    config.put("errors.transient.exhausted", "DLQ");
    config.remove("output.bootstrap.servers");
    config.put("kafka.endpoint", "pkc.example:9092");
    config.put("kafka.api.key", "kafka-key-secret");
    config.put("kafka.api.secret", "kafka-password-secret");
    LoggerFactory.getLogger(JevSecurityBoundaryTest.class);
    Field targetStream = SimpleLogger.class.getDeclaredField("TARGET_STREAM");
    targetStream.setAccessible(true);
    PrintStream originalTarget = (PrintStream) targetStream.get(null);
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    targetStream.set(null, new PrintStream(captured, true, StandardCharsets.UTF_8));
    try {
      task.start(config);
      task.put(List.of(source("generated-state-secret")));
    } finally {
      task.stop();
      targetStream.set(null, originalTarget);
      jev.stop(0);
    }

    assertEquals(1, output.history().size());
    String deadLetter = output.history().get(0).value();
    assertFalse(deadLetter.contains("jev-credential-secret"));
    assertFalse(deadLetter.contains("kafka-key-secret"));
    assertFalse(deadLetter.contains("kafka-password-secret"));
    assertFalse(deadLetter.contains("private-question-secret"));
    assertFalse(deadLetter.contains(remoteBody));
    String logs = captured.toString(StandardCharsets.UTF_8);
    assertFalse(logs.contains("jev-credential-secret"));
    assertFalse(logs.contains("kafka-key-secret"));
    assertFalse(logs.contains("kafka-password-secret"));
    assertFalse(logs.contains("generated-state-secret"));
    assertFalse(logs.contains(remoteBody));
  }

  private Map<String, String> localConfig(String endpoint) {
    return new HashMap<>(
        Map.ofEntries(
            Map.entry("name", "security-test"),
            Map.entry("topics", "input"),
            Map.entry("output.topic", "output"),
            Map.entry("errors.deadletter.topic", "dlq"),
            Map.entry("question.set.id", "security/v1"),
            Map.entry("jev.api.key", "test-key"),
            Map.entry("jev.questions", "{\"safe\":{\"type\":\"boolean\"}}"),
            Map.entry("state.mode", "FULL_VALUE"),
            Map.entry("jev.endpoint", endpoint),
            Map.entry("jev.allow.insecure.http", "true"),
            Map.entry("output.bootstrap.servers", "unused:9092")));
  }

  private SinkRecord source(String value) {
    return new SinkRecord(
        "input", 0, null, "key", null, value, 1,
        Instant.parse("2026-10-02T13:45:12Z").toEpochMilli(), TimestampType.CREATE_TIME);
  }

  private MockProducer<String, String> output() {
    return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
  }
}
