package io.github.smatiolids.kafkajev;

import java.util.regex.Pattern;

/** A requested Jev model classified for deterministic identity before a response exists. */
final class ModelReference {
  private static final Pattern VERSIONED_ID =
      Pattern.compile("^jev-[0-9]+\\.[0-9]+\\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?$");

  private final String value;
  private final Kind kind;

  private ModelReference(String value, Kind kind) {
    if (value == null || value.isBlank() || !value.equals(value.strip())) {
      throw new IllegalArgumentException("Model reference must be non-blank and unpadded");
    }
    this.value = value;
    this.kind = kind;
  }

  enum Kind {
    ALIAS,
    PINNED
  }

  static ModelReference parse(String value) {
    Kind kind = VERSIONED_ID.matcher(value).matches() ? Kind.PINNED : Kind.ALIAS;
    return new ModelReference(value, kind);
  }

  String value() {
    return value;
  }

  String failedEvaluationIdentity() {
    return kind == Kind.PINNED ? value : "unresolved:" + value;
  }
}
