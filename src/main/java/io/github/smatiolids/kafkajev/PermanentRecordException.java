package io.github.smatiolids.kafkajev;

/** A sanitized, record-specific failure that can safely cross the dead-letter boundary. */
final class PermanentRecordException extends RuntimeException {
  private final String category;
  private final String code;
  private final String state;

  PermanentRecordException(String category, String code, String message) {
    this(category, code, message, null);
  }

  PermanentRecordException(String category, String code, String message, String state) {
    super(message);
    this.category = category;
    this.code = code;
    this.state = state;
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
}
