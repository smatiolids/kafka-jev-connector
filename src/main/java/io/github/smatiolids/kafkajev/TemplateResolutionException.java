package io.github.smatiolids.kafkajev;

import org.apache.kafka.connect.errors.ConnectException;

/** A record-specific failure while building Evaluation State from a validated template. */
final class TemplateResolutionException extends ConnectException {
  TemplateResolutionException(String message) {
    super(message);
  }

  TemplateResolutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
