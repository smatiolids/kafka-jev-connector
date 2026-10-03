package io.github.smatiolids.kafkajev;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

final class SmokeEnvironmentIT {
  @Test
  void repositoryProvidesACompleteReleaseLevelSmokeWorkflow() throws Exception {
    String compose = Files.readString(Path.of("compose.yaml"));
    String connectImage = Files.readString(Path.of("local-kafka/connect.Dockerfile"));
    String fakeJev = Files.readString(Path.of("local-kafka/fake-jev.py"));
    String smoke = Files.readString(Path.of("scripts/smoke-test.sh"));

    assertTrue(compose.contains("apache/kafka:4.2.0"));
    assertTrue(compose.contains("fake-jev"));
    assertTrue(connectImage.contains("kafka-jev-connector-0.1-plugin.zip"));
    assertTrue(fakeJev.contains("permanent-secret-marker"));
    assertTrue(fakeJev.contains("transient-secret-marker"));
    assertTrue(smoke.contains("TRANSIENT_UNCOMMITTED_OK"));
    assertTrue(smoke.contains("LOG_BOUNDARY_OK"));
  }
}
