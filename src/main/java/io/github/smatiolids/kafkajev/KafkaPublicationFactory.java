package io.github.smatiolids.kafkajev;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.errors.ConnectException;

/** Owns mandatory Kafka output-client settings and pre-created-topic enforcement. */
final class KafkaPublicationFactory {
  private final JevConnectorConfig config;
  private final Function<Map<String, Object>, Producer<String, String>> producerFactory;
  private final TopicExistenceValidator topicExistenceValidator;

  KafkaPublicationFactory(
      JevConnectorConfig config,
      Function<Map<String, Object>, Producer<String, String>> producerFactory,
      TopicExistenceValidator topicExistenceValidator) {
    this.config = config;
    this.producerFactory = producerFactory;
    this.topicExistenceValidator = topicExistenceValidator;
  }

  Producer<String, String> create(Set<String> publicationTopics) {
    Map<String, Object> properties = mandatoryProperties(config);
    topicExistenceValidator.validate(publicationTopics, properties);
    return producerFactory.apply(properties);
  }

  static Producer<String, String> createKafkaProducer(Map<String, Object> properties) {
    return new KafkaProducer<>(properties);
  }

  static Map<String, Object> mandatoryProperties(JevConnectorConfig config) {
    Properties properties = new Properties();
    properties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        config.usesCloudKafkaEndpoint()
            ? config.getString(JevConnectorConfig.KAFKA_ENDPOINT).trim()
            : config.getString(JevConnectorConfig.OUTPUT_BOOTSTRAP).trim());
    if (config.hasKafkaCredentials()) {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
      properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
      properties.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
              + escapeJaas(config.getPassword(JevConnectorConfig.KAFKA_API_KEY).value())
              + "\" password=\""
              + escapeJaas(config.getPassword(JevConnectorConfig.KAFKA_API_SECRET).value())
              + "\";");
    } else {
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
    }
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    @SuppressWarnings({"unchecked", "rawtypes"})
    Map<String, Object> result = (Map) properties;
    return result;
  }

  static void validatePrecreatedTopics(
      Set<String> topics, Map<String, Object> clientProperties) {
    try (Admin admin = Admin.create(clientProperties)) {
      admin.describeTopics(topics).allTopicNames().get();
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while verifying pre-created topics", failure);
    } catch (ExecutionException | RuntimeException failure) {
      throw new ConnectException("Configured topics must be pre-created", failure);
    }
  }

  private static String escapeJaas(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  @FunctionalInterface
  interface TopicExistenceValidator {
    void validate(Set<String> topics, Map<String, Object> clientProperties);
  }
}
