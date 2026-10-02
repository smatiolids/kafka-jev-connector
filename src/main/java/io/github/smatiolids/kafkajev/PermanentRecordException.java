package io.github.smatiolids.kafkajev;

/** A sanitized, record-specific failure that can safely cross the dead-letter boundary. */
final class PermanentRecordException extends RuntimeException {
  private final String category;
  private final String code;
  private final String state;
  private final int attemptCount;

  PermanentRecordException(String category, String code, String message) {
    this(category, code, message, null);
  }

  PermanentRecordException(String category, String code, String message, String state) {
    this(category, code, message, state, 0);
  }

  PermanentRecordException(
      String category, String code, String message, String state, int attemptCount) {
    super(message);
    this.category = category;
    this.code = code;
    this.state = state;
    this.attemptCount = attemptCount;
  }

  String category() {
    return category;
  }

  String code() {
    return code;
  }

  String state() {
    return state;
  }

  int attemptCount() {
    return attemptCount;
  }
}
