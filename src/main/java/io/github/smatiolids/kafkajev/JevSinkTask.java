package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public final class JevSinkTask extends SinkTask {
  private static final String EVALUATION_ID_HEADER = "kafka-jev-evaluation-id";
  private static final String RESOLVED_MODEL_HEADER = "kafka-jev-resolved-model";

  private final Function<Map<String, Object>, Producer<String, String>> producerFactory;
  private final KafkaPublicationFactory.TopicExistenceValidator topicExistenceValidator;
  private final JevClient.Sleeper sleeper;
  private final DoubleSupplier jitterMultiplier;
  private final LongSupplier monotonicNanos;
  private final ConnectValueCanonicalizer canonicalizer = new ConnectValueCanonicalizer();
  private JevConnectorConfig config;
  private EvaluationEnvelopeFactory envelopes;
  private Producer<String, String> producer;
  private JevClient jevClient;
  private ExecutorService batchWorkers;
  private volatile boolean batchFailed;

  public JevSinkTask() {
    this(
        KafkaPublicationFactory::createKafkaProducer,
        KafkaPublicationFactory::validatePrecreatedTopics);
  }

  JevSinkTask(Function<Map<String, Object>, Producer<String, String>> producerFactory) {
    this(
        producerFactory,
        (topics, properties) -> {},
        Thread::sleep,
        () -> 0.5 + ThreadLocalRandom.current().nextDouble(),
        System::nanoTime);
  }

  JevSinkTask(
      Function<Map<String, Object>, Producer<String, String>> producerFactory,
      KafkaPublicationFactory.TopicExistenceValidator topicExistenceValidator) {
    this(
        producerFactory,
        topicExistenceValidator,
        Thread::sleep,
        () -> 0.5 + ThreadLocalRandom.current().nextDouble(),
        System::nanoTime);
  }

  JevSinkTask(
      Function<Map<String, Object>, Producer<String, String>> producerFactory,
      JevClient.Sleeper sleeper,
      DoubleSupplier jitterMultiplier,
      LongSupplier monotonicNanos) {
    this(producerFactory, (topics, properties) -> {}, sleeper, jitterMultiplier, monotonicNanos);
  }

  private JevSinkTask(
      Function<Map<String, Object>, Producer<String, String>> producerFactory,
      KafkaPublicationFactory.TopicExistenceValidator topicExistenceValidator,
      JevClient.Sleeper sleeper,
      DoubleSupplier jitterMultiplier,
      LongSupplier monotonicNanos) {
    this.producerFactory = producerFactory;
    this.topicExistenceValidator = topicExistenceValidator;
    this.sleeper = sleeper;
    this.jitterMultiplier = jitterMultiplier;
    this.monotonicNanos = monotonicNanos;
  }

  @Override
  public void start(Map<String, String> properties) {
    config = new JevConnectorConfig(properties);
    envelopes = new EvaluationEnvelopeFactory(config);
    producer =
        new KafkaPublicationFactory(config, producerFactory, topicExistenceValidator)
            .create(Set.of(config.outputTopic(), config.deadLetterTopic()));
    batchWorkers =
        Executors.newFixedThreadPool(config.getInt(JevConnectorConfig.MAX_IN_FLIGHT));
    jevClient = new JevClient(config, sleeper, jitterMultiplier);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    batchFailed = false;
    try {
      List<Future<?>> outcomes = new ArrayList<>(records.size());
      for (SinkRecord sourceRecord : records) {
        outcomes.add(batchWorkers.submit(() -> evaluateAndPublish(sourceRecord)));
      }
      RuntimeException firstFailure = null;
      boolean interrupted = false;
      for (Future<?> outcome : outcomes) {
        boolean completed = false;
        while (!completed) {
          try {
            outcome.get();
            completed = true;
          } catch (InterruptedException interruption) {
            interrupted = true;
            if (firstFailure == null) {
              firstFailure =
                  new ConnectException(
                      "Interrupted while processing Source Record batch", interruption);
            }
          } catch (ExecutionException failedRecord) {
            completed = true;
            if (firstFailure == null) {
              Throwable cause = failedRecord.getCause();
              firstFailure =
                  cause instanceof RuntimeException runtime
                      ? runtime
                      : new ConnectException("Failed to process Source Record batch", cause);
            }
          }
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      if (firstFailure != null) {
        throw firstFailure;
      }
    } catch (RuntimeException failure) {
      batchFailed = true;
      throw failure;
    }
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    return batchFailed ? Map.of() : super.preCommit(currentOffsets);
  }

  @Override
  public void stop() {
    if (batchWorkers != null) {
      batchWorkers.shutdownNow();
    }
    if (producer != null) {
      producer.close();
    }
  }

  @Override
  public String version() {
    return Version.VALUE;
  }

  private void evaluateAndPublish(SinkRecord sourceRecord) {
    if (sourceRecord.value() == null
        && config.tombstoneBehavior() == JevConnectorConfig.TombstoneBehavior.IGNORE) {
      return;
    }
    if (sourceRecord.value() == null
        && config.tombstoneBehavior() == JevConnectorConfig.TombstoneBehavior.FAIL) {
      throw new ConnectException("Tombstone Source Record configured to fail the task");
    }
    long startedAt = monotonicNanos.getAsLong();
    JsonNode canonicalValue =
        canonicalizer.canonicalize(sourceRecord.valueSchema(), sourceRecord.value());
    JsonNode canonicalKey = canonicalizer.canonicalize(sourceRecord.keySchema(), sourceRecord.key());
    if (sourceRecord.value() == null) {
      publishDeadLetter(
          sourceRecord,
          canonicalKey,
          canonicalValue,
          new PermanentRecordException(
              "STATE_BUILDING", "TOMBSTONE", "Tombstone Source Record configured for dead letter"),
          elapsedMillis(startedAt));
      return;
    }

    String state;
    try {
      state = evaluationState(sourceRecord, canonicalKey, canonicalValue);
      long maximumBytes = config.getLong(JevConnectorConfig.STATE_MAX_BYTES);
      if (maximumBytes > 0 && state.getBytes(StandardCharsets.UTF_8).length > maximumBytes) {
        throw new PermanentRecordException(
            "STATE_BUILDING",
            "STATE_TOO_LARGE",
            "Evaluation State exceeds the configured byte limit",
            state);
      }
    } catch (TemplateResolutionException failure) {
      String code =
          "Template header is not valid UTF-8".equals(failure.getMessage())
              ? "INVALID_UTF8"
              : "MISSING_TEMPLATE_VALUE";
      publishDeadLetter(
          sourceRecord,
          canonicalKey,
          canonicalValue,
          new PermanentRecordException("STATE_BUILDING", code, failure.getMessage()),
          elapsedMillis(startedAt));
      return;
    } catch (PermanentRecordException failure) {
      publishDeadLetter(
          sourceRecord, canonicalKey, canonicalValue, failure, elapsedMillis(startedAt));
      return;
    }

    JevClient.Evaluation success;
    try {
      success = jevClient.evaluate(state);
    } catch (PermanentRecordException failure) {
      publishDeadLetter(
          sourceRecord, canonicalKey, canonicalValue, failure, elapsedMillis(startedAt));
      return;
    }
    long durationMillis = elapsedMillis(startedAt);
    EvaluationEnvelopeFactory.BuiltEnvelope enriched =
        envelopes.enriched(
            sourceRecord,
            canonicalKey,
            canonicalValue,
            state,
            success.inferenceResult(),
            success.attemptCount(),
            durationMillis);
    String key =
        config.outputKeyMode() == JevConnectorConfig.OutputKeyMode.EVALUATION_ID
            ? enriched.evaluationId()
            : sourceRecord.key() == null ? null : render(canonicalKey);
    ProducerRecord<String, String> output =
        new ProducerRecord<>(
            config.outputTopic(),
            null,
            sourceRecord.timestamp(),
            key,
            CanonicalJson.write(enriched.json()),
            enrichedHeaders(sourceRecord, enriched.evaluationId(), enriched.effectiveModel()));
    publish(output, "Enriched Record");
  }

  private void publishDeadLetter(
      SinkRecord sourceRecord,
      JsonNode canonicalKey,
      JsonNode canonicalValue,
      PermanentRecordException failure,
      long durationMillis) {
    EvaluationEnvelopeFactory.BuiltEnvelope deadLetter =
        envelopes.deadLetter(
            sourceRecord, canonicalKey, canonicalValue, failure, durationMillis);

    publish(
        new ProducerRecord<>(
            config.deadLetterTopic(),
            null,
            sourceRecord.timestamp(),
            deadLetter.sourceId(),
            CanonicalJson.write(deadLetter.json()),
            provenanceHeaders(deadLetter.evaluationId(), deadLetter.effectiveModel())),
        "Dead-Letter Record");
  }

  private RecordHeaders enrichedHeaders(
      SinkRecord sourceRecord, String evaluationId, String resolvedModel) {
    RecordHeaders headers = new RecordHeaders();
    if (config.outputHeadersMode() == JevConnectorConfig.OutputHeadersMode.COPY) {
      sourceRecord.headers().forEach(header -> headers.add(header.key(), outputHeaderValue(header)));
    }
    headers.add(EVALUATION_ID_HEADER, evaluationId.getBytes(StandardCharsets.UTF_8));
    headers.add(RESOLVED_MODEL_HEADER, resolvedModel.getBytes(StandardCharsets.UTF_8));
    return headers;
  }

  private RecordHeaders provenanceHeaders(String evaluationId, String resolvedModel) {
    RecordHeaders headers = new RecordHeaders();
    headers.add(EVALUATION_ID_HEADER, evaluationId.getBytes(StandardCharsets.UTF_8));
    headers.add(RESOLVED_MODEL_HEADER, resolvedModel.getBytes(StandardCharsets.UTF_8));
    return headers;
  }

  private byte[] outputHeaderValue(org.apache.kafka.connect.header.Header header) {
    Object value = header.value();
    if (value == null) {
      return null;
    }
    if (value instanceof byte[] bytes) {
      return bytes.clone();
    }
    if (value instanceof ByteBuffer buffer) {
      ByteBuffer copy = buffer.duplicate();
      byte[] bytes = new byte[copy.remaining()];
      copy.get(bytes);
      return bytes;
    }
    if (value instanceof String text) {
      return text.getBytes(StandardCharsets.UTF_8);
    }
    return render(canonicalizer.canonicalize(header.schema(), value)).getBytes(StandardCharsets.UTF_8);
  }

  private void publish(ProducerRecord<String, String> record, String recordType) {
    try {
      producer.send(record).get();
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while publishing " + recordType, error);
    } catch (ExecutionException error) {
      throw new ConnectException("Failed to publish " + recordType, error.getCause());
    } catch (RuntimeException error) {
      throw new ConnectException("Failed to publish " + recordType, error);
    }
  }

  private String fullValueState(SinkRecord sourceRecord, JsonNode canonicalValue) {
    if (sourceRecord.value() == null) {
      throw new ConnectException("Null Source Record values require a configured tombstone policy");
    }
    if (isRawBytes(sourceRecord.valueSchema(), sourceRecord.value())) {
      if (config.rawBytesEncoding() != JevConnectorConfig.RawBytesEncoding.UTF8) {
        throw new PermanentRecordException(
            "STATE_BUILDING",
            "RAW_BYTES_DISABLED",
            "Raw byte Evaluation State requires state.raw_bytes.encoding=UTF-8");
      }
      return StrictUtf8.decodeRawState(sourceRecord.value());
    }
    return render(canonicalValue);
  }

  private String evaluationState(
      SinkRecord sourceRecord, JsonNode canonicalKey, JsonNode canonicalValue) {
    if (config.stateTemplate() != null) {
      return config
          .stateTemplate()
          .render(
              sourceRecord,
              canonicalKey,
              canonicalValue,
              config.rawBytesEncoding());
    }
    return fullValueState(sourceRecord, canonicalValue);
  }

  private static boolean isRawBytes(Schema schema, Object value) {
    return (schema != null && schema.type() == Schema.Type.BYTES)
        || value instanceof byte[]
        || value instanceof ByteBuffer;
  }

  private static String render(JsonNode value) {
    if (value.isTextual()) {
      return value.textValue();
    }
    return CanonicalJson.serialize(value);
  }

  private long elapsedMillis(long startedAt) {
    return Math.max(1, (monotonicNanos.getAsLong() - startedAt + 999_999) / 1_000_000);
  }

}
