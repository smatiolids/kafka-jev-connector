package io.github.smatiolids.kafkajev;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

class JevSinkConnectorTest {
  @Test
  void exposesTheCompleteConfigurationSurfaceAndMarksEveryCredentialSensitive() {
    Map<String, ConfigDef.ConfigKey> keys = new JevSinkConnector((topics, properties) -> {}).config().configKeys();

    assertEquals(
        Set.of(
            "name", "topics", "output.topic", "errors.deadletter.topic", "question.set.id",
            "jev.api.key", "jev.questions", "state.mode", "state.template",
            "jev.endpoint", "jev.allow.insecure.http", "jev.model", "jev.max.in.flight",
            "jev.connect.timeout.ms", "jev.request.timeout.ms", "jev.retry.max.attempts",
            "jev.retry.initial.backoff.ms", "jev.retry.max.retry_after.ms",
            "state.raw_bytes.encoding", "state.max.bytes", "output.key.mode",
            "output.headers.mode", "behavior.on.null.values", "errors.transient.exhausted",
            "output.bootstrap.servers", "kafka.endpoint", "kafka.api.key", "kafka.api.secret"),
        keys.keySet());
    assertEquals(ConfigDef.Type.PASSWORD, keys.get("jev.api.key").type);
    assertEquals(ConfigDef.Type.PASSWORD, keys.get("kafka.api.key").type);
    assertEquals(ConfigDef.Type.PASSWORD, keys.get("kafka.api.secret").type);
  }

  @Test
  void rejectsMissingAndBlankRequiredConnectorPropertiesAtStartup() {
    for (String required :
        List.of(
            "name", "topics", "output.topic", "errors.deadletter.topic", "question.set.id",
            "jev.api.key", "jev.questions", "state.mode")) {
      Map<String, String> missing = localConfig();
      missing.remove(required);
      assertThrows(ConfigException.class, () -> connector().start(missing), "missing " + required);

      Map<String, String> blank = localConfig();
      blank.put(required, "   ");
      assertThrows(ConfigException.class, () -> connector().start(blank), "blank " + required);
    }
    assertThrows(ConfigException.class, () -> connector().start(with("jev.model", "   ")));
    assertThrows(ConfigException.class, () -> connector().start(with("jev.model", " jev-latest ")));
  }

  @Test
  void rejectsContradictoryTopicsRegexAndStatePolicyConfiguration() {
    for (Map<String, String> invalid :
        List.of(
            with("topics", "support-input,,audit-input"),
            with("topics", "support-input,support-input"),
            with("output.topic", "support-input"),
            with("errors.deadletter.topic", "support-output"),
            with("topics.regex", "support-.*"),
            with("state.mode", "TEMPLATE"),
            with("state.template", "not allowed for full value"))) {
      assertThrows(ConfigException.class, () -> connector().start(invalid));
    }

    Map<String, String> template = with("state.mode", "TEMPLATE");
    template.put("state.template", "safe=${value:/safe}");
    connector().start(template);
  }

  @Test
  void rejectsNonHttpsEndpointsUnlessLocalHttpIsExplicitlyEnabled() {
    for (String endpoint :
        List.of("http://127.0.0.1:8080/evaluate", "/evaluate", "ftp://example.test/evaluate")) {
      Map<String, String> invalid = with("jev.endpoint", endpoint);
      assertThrows(ConfigException.class, () -> connector().start(invalid));
    }

    Map<String, String> localHttp = with("jev.endpoint", "http://127.0.0.1:8080/evaluate");
    localHttp.put("jev.allow.insecure.http", "true");
    connector().start(localHttp);
  }

  @Test
  void validatesThatEveryExplicitTopicAlreadyExistsBeforeTasksAreCreated() {
    AtomicReference<Set<String>> checked = new AtomicReference<>();
    JevSinkConnector connector =
        new JevSinkConnector((topics, properties) -> checked.set(Set.copyOf(topics)));
    Map<String, String> config = with("topics", " support-input , audit-input ");

    connector.start(config);

    assertEquals(Set.of("support-input", "audit-input", "support-output", "support-dlq"), checked.get());

    JevSinkConnector missing =
        new JevSinkConnector(
            (topics, properties) -> {
              throw new ConnectException("Configured topics must be pre-created");
            });
    assertThrows(ConnectException.class, () -> missing.start(config));
  }

  @Test
  void compilesEveryBoundedDestinationPolicyAndEnumeratesItsCandidateTopics() {
    Map<String, String> base = routingConfig();
    Map<String, Set<String>> cases =
        Map.of(
            "events-${answers.department.choice}",
                Set.of("events-billing", "events-technical", "events-sales"),
            "risk-${answers.urgency.level}", Set.of("risk-0", "risk-1", "risk-2"),
            "queue-${answers.escalate.noul >= 0.5; \"human\", \"auto\"}",
                Set.of("queue-human", "queue-auto"),
            "priority-${answers.urgency.score < 1.5; \"normal\", \"urgent\"}",
                Set.of("priority-normal", "priority-urgent"));

    for (Map.Entry<String, Set<String>> testCase : cases.entrySet()) {
      AtomicReference<Set<String>> checked = new AtomicReference<>();
      Map<String, String> config = new HashMap<>(base);
      config.put("output.topic", testCase.getKey());
      new JevSinkConnector((topics, properties) -> checked.set(Set.copyOf(topics))).start(config);

      Set<String> expected = new java.util.HashSet<>(testCase.getValue());
      expected.add("support-input");
      expected.add("support-dlq");
      assertEquals(expected, checked.get());
    }

    for (String operator : List.of(">", ">=", "<", "<=")) {
      Map<String, String> config = new HashMap<>(base);
      config.put(
          "output.topic",
          "route-${answers.escalate.noul " + operator + " 0.5; \"yes\", \"no\"}");
      connector().start(config);
    }
  }

  @Test
  void rejectsUnboundedMalformedMismatchedAndUnreachableDestinationPolicies() {
    Map<String, String> base = routingConfig();
    for (String policy :
        List.of(
            "${answers.missing.choice}",
            "${answers.urgency.choice}",
            "${answers.department.score}",
            "${answers.department.choice}-${answers.department.choice}",
            "${answers.department}",
            "${answers.department.choice == \"billing\"; \"a\", \"b\"}",
            "${answers.urgency.score}",
            "${answers.escalate.noul == 0.5; \"a\", \"b\"}",
            "${answers.escalate.noul > 1; \"a\", \"b\"}",
            "${answers.urgency.score <= 2; \"a\", \"b\"}",
            "${answers.escalate.noul > NaN; \"a\", \"b\"}",
            "${answers.escalate.noul > 0.5; answers.department.choice, \"b\"}",
            "bad topic-${answers.department.choice}")) {
      Map<String, String> config = new HashMap<>(base);
      config.put("output.topic", policy);
      assertThrows(ConfigException.class, () -> connector().start(config), policy);
    }

    Map<String, String> overlap = new HashMap<>(base);
    overlap.put("output.topic", "support-${answers.department.choice}");
    overlap.put("topics", "support-input,support-billing");
    assertThrows(ConfigException.class, () -> connector().start(overlap));
  }

  @Test
  void selectsLocalOrCloudKafkaConnectionWithoutAcceptingProducerOverrides() {
    AtomicReference<Map<String, Object>> localProperties = new AtomicReference<>();
    JevSinkTask localTask =
        new JevSinkTask(
            properties -> {
              localProperties.set(Map.copyOf(properties));
              return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
            });
    Map<String, String> attemptedOverride = localConfig();
    attemptedOverride.put("producer.override.security.protocol", "SASL_PLAINTEXT");
    localTask.start(attemptedOverride);

    assertEquals("local-kafka:9092", localProperties.get().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("PLAINTEXT", localProperties.get().get("security.protocol"));
    assertTrue(localProperties.get().keySet().stream().noneMatch(key -> key.startsWith("producer.override.")));
    localTask.stop();

    AtomicReference<Map<String, Object>> cloudProperties = new AtomicReference<>();
    JevSinkTask cloudTask =
        new JevSinkTask(
            properties -> {
              cloudProperties.set(Map.copyOf(properties));
              return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
            });
    cloudTask.start(cloudConfig());

    assertEquals("pkc.example:9092", cloudProperties.get().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("SASL_SSL", cloudProperties.get().get("security.protocol"));
    assertEquals("PLAIN", cloudProperties.get().get("sasl.mechanism"));
    String jaas = cloudProperties.get().get("sasl.jaas.config").toString();
    assertTrue(jaas.contains("username=\"cloud-key\""));
    assertTrue(jaas.contains("password=\"cloud-secret\""));
    cloudTask.stop();

    AtomicReference<Map<String, Object>> securedLocalProperties = new AtomicReference<>();
    JevSinkTask securedLocalTask =
        new JevSinkTask(
            properties -> {
              securedLocalProperties.set(Map.copyOf(properties));
              return new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
            });
    Map<String, String> securedLocal = localConfig();
    securedLocal.put("kafka.api.key", "local-key");
    securedLocal.put("kafka.api.secret", "local-secret");
    securedLocalTask.start(securedLocal);
    assertEquals("local-kafka:9092", securedLocalProperties.get().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("SASL_SSL", securedLocalProperties.get().get("security.protocol"));
    securedLocalTask.stop();
  }

  @Test
  void rejectsMissingAmbiguousAndPartialKafkaConnections() {
    Map<String, String> none = localConfig();
    none.remove("output.bootstrap.servers");
    assertThrows(ConfigException.class, () -> connector().start(none));

    Map<String, String> partialCloud = new HashMap<>(none);
    partialCloud.put("kafka.endpoint", "pkc.example:9092");
    partialCloud.put("kafka.api.key", "cloud-key");
    assertThrows(ConfigException.class, () -> connector().start(partialCloud));

    Map<String, String> ambiguous = localConfig();
    ambiguous.put("kafka.endpoint", "pkc.example:9092");
    assertThrows(ConfigException.class, () -> connector().start(ambiguous));
  }

  private JevSinkConnector connector() {
    return new JevSinkConnector((topics, properties) -> {});
  }

  private Map<String, String> with(String key, String value) {
    Map<String, String> result = localConfig();
    result.put(key, value);
    return result;
  }

  private Map<String, String> localConfig() {
    return new HashMap<>(
        Map.ofEntries(
            Map.entry("name", "support-evaluator"),
            Map.entry("topics", "support-input"),
            Map.entry("output.topic", "support-output"),
            Map.entry("errors.deadletter.topic", "support-dlq"),
            Map.entry("question.set.id", "support-routing/v1"),
            Map.entry("jev.api.key", "jev-secret"),
            Map.entry("jev.questions", "{\"department\":{\"type\":\"choice\"}}"),
            Map.entry("state.mode", "FULL_VALUE"),
            Map.entry("output.bootstrap.servers", "local-kafka:9092")));
  }

  private Map<String, String> cloudConfig() {
    Map<String, String> result = localConfig();
    result.remove("output.bootstrap.servers");
    result.put("kafka.endpoint", "pkc.example:9092");
    result.put("kafka.api.key", "cloud-key");
    result.put("kafka.api.secret", "cloud-secret");
    return result;
  }

  private Map<String, String> routingConfig() {
    Map<String, String> result = localConfig();
    result.put(
        "jev.questions",
        "{\"department\":{\"type\":\"choice\",\"criteria\":{\"billing\":\"B\",\"technical\":\"T\",\"sales\":\"S\"}},"
            + "\"urgency\":{\"type\":\"score\",\"criteria\":[\"low\",\"medium\",\"high\"]},"
            + "\"escalate\":{\"type\":\"noul\"}}");
    return result;
  }
}
