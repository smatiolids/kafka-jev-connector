package io.github.smatiolids.kafkajev;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Test;

final class PluginPackagingIT {
  private static final String VERSION = System.getProperty("pluginVersion");
  private static final String ROOT = "kafka-jev-connector-" + VERSION + "/";
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void buildProducesSelfDescribingConfluentPluginWithoutPlatformLibraries() throws Exception {
    Path target = Path.of("target");
    assertTrue(Files.isRegularFile(target.resolve("kafka-jev-connector-" + VERSION + ".jar")));

    Path plugin = target.resolve("kafka-jev-connector-" + VERSION + "-plugin.zip");
    assertTrue(Files.isRegularFile(plugin), "Maven package must produce the plugin ZIP");

    try (ZipFile zip = new ZipFile(plugin.toFile())) {
      Set<String> entries = entryNames(zip);

      assertContains(entries,
          "manifest.json",
          "lib/kafka-jev-connector-" + VERSION + ".jar",
          "doc/LICENSE",
          "doc/NOTICE",
          "doc/THIRD-PARTY-NOTICES",
          "doc/README.md",
          "doc/SETUP.md",
          "doc/configuration.md",
          "doc/record-contracts.md",
          "etc/local-connector.json",
          "etc/confluent-cloud-connector.json");

      assertBundled(entries, "jackson-annotations-");
      assertBundled(entries, "jackson-core-");
      assertBundled(entries, "jackson-databind-");
      assertBundled(entries, "java-json-canonicalization-");
      assertNotBundled(entries, "connect-api-");
      assertNotBundled(entries, "kafka-clients-");
      assertNotBundled(entries, "slf4j-api-");
      assertNotBundled(entries, "slf4j-simple-");
      assertNotBundled(entries, "log4j-api-");
      assertNotBundled(entries, "log4j-core-");
      assertNotBundled(entries, "logback-classic-");

      JsonNode manifest = json(zip, ROOT + "manifest.json");
      assertEquals("kafka-jev-connector", manifest.path("name").asText());
      assertEquals("Kafka Jev Connector", manifest.path("title").asText());
      assertEquals(VERSION, manifest.path("version").asText());
      assertEquals("4.2.0", manifest.path("kafka_version").asText());
      assertEquals("17", manifest.path("java_version").asText());
      assertEquals("sink", manifest.path("component_types").path(0).asText());
      assertEquals(JevSinkConnector.class.getName(), manifest.path("connector_class").asText());
      assertSensitive(manifest, "jev.api.key", "kafka.api.key", "kafka.api.secret");

      assertLocalSample(json(zip, ROOT + "etc/local-connector.json"));
      assertCloudSample(json(zip, ROOT + "etc/confluent-cloud-connector.json"));
    }
  }

  private static void assertLocalSample(JsonNode sample) {
    JsonNode config = sample.path("config");
    assertEquals(JevSinkConnector.class.getName(), config.path("connector.class").asText());
    assertEquals("kafka:9092", config.path("output.bootstrap.servers").asText());
    assertEquals("http://fake-jev:8080/v1/systemone", config.path("jev.endpoint").asText());
    assertTrue(config.path("jev.allow.insecure.http").asBoolean());
    assertCommonConnectorConfig(config);
  }

  private static void assertCloudSample(JsonNode sample) {
    JsonNode config = sample.path("config");
    assertEquals("CUSTOM", config.path("confluent.connector.type").asText());
    assertEquals("4.2", config.path("confluent.custom.connect.plugin.runtime").asText());
    assertEquals("17", config.path("confluent.custom.connect.java.version").asText());
    assertEquals("api.typesafe.ai:443", config.path("confluent.custom.connection.endpoints").asText());
    assertFalse(config.has("output.bootstrap.servers"));
    assertFalse(config.path("kafka.api.key").asText().isBlank());
    assertFalse(config.path("kafka.api.secret").asText().isBlank());
    assertEquals("https://api.typesafe.ai/v1/systemone", config.path("jev.endpoint").asText());
    assertFalse(config.path("jev.allow.insecure.http").asBoolean());
    assertCommonConnectorConfig(config);
  }

  private static void assertCommonConnectorConfig(JsonNode config) {
    assertEquals("jev-input", config.path("topics").asText());
    assertEquals("jev-output", config.path("output.topic").asText());
    assertEquals("jev-dlq", config.path("errors.deadletter.topic").asText());
    assertEquals("org.apache.kafka.connect.storage.StringConverter", config.path("key.converter").asText());
    assertEquals("org.apache.kafka.connect.json.JsonConverter", config.path("value.converter").asText());
    assertEquals("false", config.path("value.converter.schemas.enable").asText());
    assertFalse(config.path("jev.api.key").asText().isBlank());
    assertFalse(config.path("jev.questions").asText().isBlank());
    assertFalse(config.path("question.set.id").asText().isBlank());
    assertFalse(config.path("state.mode").asText().isBlank());
  }

  private static Set<String> entryNames(ZipFile zip) {
    Set<String> names = new HashSet<>();
    zip.stream().map(ZipEntry::getName).forEach(names::add);
    return names;
  }

  private static void assertContains(Set<String> entries, String... relativeNames) {
    for (String relativeName : relativeNames) {
      assertTrue(entries.contains(ROOT + relativeName), "missing " + ROOT + relativeName);
    }
  }

  private static void assertBundled(Set<String> entries, String jarPrefix) {
    assertTrue(entries.stream().anyMatch(name -> name.startsWith(ROOT + "lib/" + jarPrefix)),
        "missing runtime dependency " + jarPrefix);
  }

  private static void assertNotBundled(Set<String> entries, String jarPrefix) {
    assertFalse(entries.stream().anyMatch(name -> name.startsWith(ROOT + "lib/" + jarPrefix)),
        "runtime-provided dependency was bundled: " + jarPrefix);
  }

  private static JsonNode json(ZipFile zip, String entryName) throws IOException {
    ZipEntry entry = zip.getEntry(entryName);
    assertNotNull(entry, "missing " + entryName);
    try (var input = zip.getInputStream(entry)) {
      return JSON.readTree(new String(input.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private static void assertSensitive(JsonNode manifest, String... properties) {
    Set<String> actual = new HashSet<>();
    manifest.path("sensitive_config_properties").forEach(node -> actual.add(node.asText()));
    for (String property : properties) {
      assertTrue(actual.contains(property), "manifest must mark " + property + " sensitive");
    }
  }
}
