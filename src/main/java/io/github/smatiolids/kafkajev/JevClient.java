package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.function.DoubleSupplier;
import org.apache.kafka.connect.errors.ConnectException;

/** Executes one logical Jev evaluation, including its bounded HTTP attempts. */
final class JevClient {
  private static final HttpHeaders EMPTY_HEADERS = HttpHeaders.of(Map.of(), (name, value) -> true);

  private final JevConnectorConfig config;
  private final Sleeper sleeper;
  private final DoubleSupplier jitterMultiplier;
  private final HttpClient http;

  JevClient(JevConnectorConfig config, Sleeper sleeper, DoubleSupplier jitterMultiplier) {
    this.config = config;
    this.sleeper = sleeper;
    this.jitterMultiplier = jitterMultiplier;
    http =
        HttpClient.newBuilder()
            .connectTimeout(
                Duration.ofMillis(config.getInt(JevConnectorConfig.CONNECT_TIMEOUT_MS)))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
  }

  Evaluation evaluate(String state) {
    HttpRequest request = request(state);
    int maximumAttempts = config.getInt(JevConnectorConfig.RETRY_MAX_ATTEMPTS);
    for (int attempt = 1; attempt <= maximumAttempts; attempt++) {
      HttpResponse<String> response = null;
      try {
        response = http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        int status = response.statusCode();
        if (status == 200) {
          return new Evaluation(parseInferenceResult(response.body()), attempt);
        }
        if (status == 413) {
          throw new PermanentRecordException(
              "JEV_REQUEST",
              "RECORD_TOO_LARGE",
              "Jev rejected the Evaluation State as too large",
              state,
              attempt);
        }
        if (isTaskFatalStatus(status)) {
          throw new ConnectException("Jev rejected the request with HTTP " + status);
        }
        if (!isTransientStatus(status)) {
          throw new ConnectException("Jev returned an unrecognized HTTP status " + status);
        }
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        throw new ConnectException("Interrupted while calling Jev", error);
      } catch (IOException transientFailure) {
        // Connection failures and per-attempt request timeouts are transient.
      }

      if (attempt == maximumAttempts) {
        exhausted(state, attempt);
      }
      sleepBeforeRetry(attempt, response == null ? EMPTY_HEADERS : response.headers());
    }
    throw new AssertionError("Configured Jev attempt limit was not applied");
  }

  private HttpRequest request(String state) {
    ObjectNode requestBody = CanonicalJson.MAPPER.createObjectNode();
    requestBody.put("state", state);
    requestBody.set("questions", config.questions());
    requestBody.put("model", config.modelReference().value());
    return HttpRequest.newBuilder(URI.create(config.getString(JevConnectorConfig.ENDPOINT)))
        .timeout(Duration.ofMillis(config.getInt(JevConnectorConfig.REQUEST_TIMEOUT_MS)))
        .header(
            "Authorization",
            "Bearer " + config.getPassword(JevConnectorConfig.API_KEY).value())
        .header("Content-Type", "application/json")
        .POST(
            HttpRequest.BodyPublishers.ofString(
                CanonicalJson.write(requestBody), StandardCharsets.UTF_8))
        .build();
  }

  private void exhausted(String state, int attempt) {
    if (config.transientExhaustedBehavior()
        == JevConnectorConfig.TransientExhaustedBehavior.DLQ) {
      throw new PermanentRecordException(
          "JEV_SERVICE",
          "TRANSIENT_EXHAUSTED",
          "Jev Evaluation exhausted transient attempts",
          state,
          attempt);
    }
    throw new ConnectException("Jev Evaluation exhausted " + attempt + " transient attempts");
  }

  private static InferenceResult parseInferenceResult(String responseBody) {
    try {
      return InferenceResult.from(CanonicalJson.MAPPER.readTree(responseBody));
    } catch (ConnectException error) {
      throw error;
    } catch (Exception error) {
      throw new ConnectException("Jev response is not valid JSON");
    }
  }

  private void sleepBeforeRetry(int completedAttempt, HttpHeaders responseHeaders) {
    long delay = Math.max(exponentialBackoff(completedAttempt), retryAfterMillis(responseHeaders));
    try {
      sleeper.sleep(delay);
    } catch (InterruptedException error) {
      Thread.currentThread().interrupt();
      throw new ConnectException("Interrupted while waiting to retry Jev", error);
    }
  }

  private long exponentialBackoff(int completedAttempt) {
    long exponential = config.getLong(JevConnectorConfig.RETRY_INITIAL_BACKOFF_MS);
    for (int i = 1; i < completedAttempt; i++) {
      exponential = exponential > Long.MAX_VALUE / 2 ? Long.MAX_VALUE : exponential * 2;
    }
    double jittered = exponential * boundedJitterMultiplier();
    return jittered >= Long.MAX_VALUE ? Long.MAX_VALUE : Math.max(0L, Math.round(jittered));
  }

  private double boundedJitterMultiplier() {
    return Math.max(0.5, Math.min(1.5, jitterMultiplier.getAsDouble()));
  }

  private long retryAfterMillis(HttpHeaders headers) {
    String configured = headers.firstValue("Retry-After").orElse(null);
    if (configured == null) {
      return 0;
    }
    long maximum = config.getLong(JevConnectorConfig.RETRY_MAX_RETRY_AFTER_MS);
    long requested;
    try {
      long seconds = Long.parseLong(configured.strip());
      if (seconds < 0) {
        return 0;
      }
      requested = seconds > maximum / 1000L ? maximum : seconds * 1000L;
    } catch (NumberFormatException notDeltaSeconds) {
      try {
        requested =
            Math.max(
                0L,
                Duration.between(
                        Instant.now(),
                        ZonedDateTime.parse(configured, DateTimeFormatter.RFC_1123_DATE_TIME)
                            .toInstant())
                    .toMillis());
      } catch (DateTimeParseException invalidHeader) {
        return 0;
      }
    }
    return Math.min(requested, maximum);
  }

  private static boolean isTaskFatalStatus(int status) {
    return status == 400 || status == 401 || status == 403 || status == 404 || status == 422;
  }

  private static boolean isTransientStatus(int status) {
    return status == 408 || status == 429 || status == 529 || (status >= 500 && status <= 599);
  }

  @FunctionalInterface
  interface Sleeper {
    void sleep(long milliseconds) throws InterruptedException;
  }

  record Evaluation(InferenceResult inferenceResult, int attemptCount) {}
}
