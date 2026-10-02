package io.github.smatiolids.kafkajev;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public final class JevSinkConnector extends SinkConnector {
  private final TopicExistenceValidator topicExistenceValidator;
  private Map<String, String> properties;

  public JevSinkConnector() {
    this(JevSinkConnector::validatePrecreatedTopics);
  }

  JevSinkConnector(TopicExistenceValidator topicExistenceValidator) {
    this.topicExistenceValidator = topicExistenceValidator;
  }

  @Override
  public void start(Map<String, String> properties) {
    JevConnectorConfig config = new JevConnectorConfig(properties);
    topicExistenceValidator.validate(config.allTopics(), JevSinkTask.producerProperties(config));
    this.properties = Map.copyOf(properties);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return JevSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int index = 0; index < maxTasks; index++) {
      configs.add(properties);
    }
    return configs;
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return JevConnectorConfig.CONFIG_DEF;
  }

  @Override
  public String version() {
    return Version.VALUE;
  }

  static void validatePrecreatedTopics(
      Set<String> topics, Map<String, Object> clientProperties) {
    try (Admin admin = Admin.create(clientProperties)) {
      admin.describeTopics(topics).allTopicNames().get();
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new org.apache.kafka.connect.errors.ConnectException(
          "Interrupted while verifying pre-created topics", failure);
    } catch (ExecutionException | RuntimeException failure) {
      throw new org.apache.kafka.connect.errors.ConnectException(
          "Configured input, output, and dead-letter topics must be pre-created", failure);
    }
  }

  @FunctionalInterface
  interface TopicExistenceValidator {
    void validate(Set<String> topics, Map<String, Object> clientProperties);
  }
}
