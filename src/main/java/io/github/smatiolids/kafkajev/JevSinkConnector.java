package io.github.smatiolids.kafkajev;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public final class JevSinkConnector extends SinkConnector {
  private final KafkaPublicationFactory.TopicExistenceValidator topicExistenceValidator;
  private Map<String, String> properties;

  public JevSinkConnector() {
    this(KafkaPublicationFactory::validatePrecreatedTopics);
  }

  JevSinkConnector(KafkaPublicationFactory.TopicExistenceValidator topicExistenceValidator) {
    this.topicExistenceValidator = topicExistenceValidator;
  }

  @Override
  public void start(Map<String, String> properties) {
    JevConnectorConfig config = new JevConnectorConfig(properties);
    topicExistenceValidator.validate(
        config.allTopics(), KafkaPublicationFactory.mandatoryProperties(config));
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

}
