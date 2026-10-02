package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

/** A validated template whose record-independent grammar is compiled once at startup. */
final class CompiledStateTemplate {
  private static final Set<String> METADATA = Set.of("topic", "partition", "offset", "timestamp");

  private final List<Segment> segments;

  private CompiledStateTemplate(List<Segment> segments) {
    this.segments = List.copyOf(segments);
  }

  static CompiledStateTemplate compile(String template) {
    if (template == null) {
      throw invalid("is required when state.mode=TEMPLATE");
    }
    List<Segment> segments = new ArrayList<>();
    StringBuilder literal = new StringBuilder();
    for (int index = 0; index < template.length(); ) {
      char current = template.charAt(index);
      if (current == '\\') {
        index = appendEscapedLiteral(template, index, literal);
      } else if (current == '$' && index + 1 < template.length() && template.charAt(index + 1) == '{') {
        if (!literal.isEmpty()) {
          segments.add(new Literal(literal.toString()));
          literal.setLength(0);
        }
        int end = placeholderEnd(template, index + 2);
        segments.add(parseReference(template.substring(index + 2, end)));
        index = end + 1;
      } else {
        literal.append(current);
        index++;
      }
    }
    if (!literal.isEmpty()) {
      segments.add(new Literal(literal.toString()));
    }
    return new CompiledStateTemplate(segments);
  }

  String render(SinkRecord record, JsonNode key, JsonNode value, boolean rawBytesUtf8) {
    StringBuilder state = new StringBuilder();
    for (Segment segment : segments) {
      segment.append(state, record, key, value, rawBytesUtf8);
    }
    return state.toString();
  }

  private static int appendEscapedLiteral(String template, int index, StringBuilder literal) {
    if (index + 1 >= template.length()) {
      throw invalid("has a trailing escape");
    }
    char escaped = template.charAt(index + 1);
    if (escaped == '\\') {
      literal.append('\\');
      return index + 2;
    }
    if (escaped == '$'
        && index + 2 < template.length()
        && template.charAt(index + 2) == '{') {
      literal.append("${");
      return index + 3;
    }
    throw invalid("contains an unsupported escape");
  }

  private static int placeholderEnd(String template, int start) {
    for (int index = start; index < template.length(); index++) {
      char current = template.charAt(index);
      if (current == '}') {
        return index;
      }
      if (current == '\\') {
        if (index + 1 >= template.length()) {
          throw invalid("has a trailing escape");
        }
        char escaped = template.charAt(index + 1);
        if (escaped == '}' || escaped == '\\') {
          index++;
        } else if (escaped == '$'
            && index + 2 < template.length()
            && template.charAt(index + 2) == '{') {
          index += 2;
        } else {
          throw invalid("contains an unsupported escape");
        }
      }
    }
    throw invalid("contains an unterminated placeholder");
  }

  private static Reference parseReference(String expression) {
    int defaultSeparator = expression.indexOf(":-");
    String selector = defaultSeparator < 0 ? expression : expression.substring(0, defaultSeparator);
    boolean hasDefault = defaultSeparator >= 0;
    String defaultValue =
        hasDefault ? decodeDefault(expression.substring(defaultSeparator + 2)) : null;

    if (selector.equals("value") || selector.equals("key")) {
      return new Reference(selector, null, null, hasDefault, defaultValue);
    }
    if (selector.startsWith("value:") || selector.startsWith("key:")) {
      int colon = selector.indexOf(':');
      String source = selector.substring(0, colon);
      String pointerText = selector.substring(colon + 1);
      validatePointer(pointerText);
      try {
        return new Reference(
            source, JsonPointer.compile(pointerText), null, hasDefault, defaultValue);
      } catch (IllegalArgumentException error) {
        throw invalid("contains a malformed JSON Pointer");
      }
    }
    if (selector.startsWith("header:")) {
      String name = selector.substring("header:".length());
      if (name.isEmpty()) {
        throw invalid("contains an empty header name");
      }
      return new Reference("header", null, name, hasDefault, defaultValue);
    }
    if (selector.startsWith("metadata:")) {
      String name = selector.substring("metadata:".length());
      if (!METADATA.contains(name)) {
        throw invalid("contains an unknown metadata name");
      }
      return new Reference("metadata", null, name, hasDefault, defaultValue);
    }
    throw invalid("contains an unknown placeholder source");
  }

  private static void validatePointer(String pointer) {
    if (!pointer.startsWith("/")) {
      throw invalid("contains a malformed JSON Pointer");
    }
    for (int index = 0; index < pointer.length(); index++) {
      if (pointer.charAt(index) == '~'
          && (index + 1 >= pointer.length()
              || (pointer.charAt(index + 1) != '0' && pointer.charAt(index + 1) != '1'))) {
        throw invalid("contains a malformed JSON Pointer");
      }
    }
  }

  private static String decodeDefault(String value) {
    StringBuilder decoded = new StringBuilder();
    for (int index = 0; index < value.length(); ) {
      char current = value.charAt(index);
      if (current != '\\') {
        decoded.append(current);
        index++;
        continue;
      }
      if (index + 1 >= value.length()) {
        throw invalid("has a trailing escape");
      }
      char escaped = value.charAt(index + 1);
      if (escaped == '}' || escaped == '\\') {
        decoded.append(escaped);
        index += 2;
      } else if (escaped == '$'
          && index + 2 < value.length()
          && value.charAt(index + 2) == '{') {
        decoded.append("${");
        index += 3;
      } else {
        throw invalid("contains an unsupported escape");
      }
    }
    return decoded.toString();
  }

  private static ConfigException invalid(String reason) {
    return new ConfigException(JevConnectorConfig.STATE_TEMPLATE, null, reason);
  }

  private sealed interface Segment permits Literal, Reference {
    void append(
        StringBuilder target,
        SinkRecord record,
        JsonNode key,
        JsonNode value,
        boolean rawBytesUtf8);
  }

  private record Literal(String text) implements Segment {
    @Override
    public void append(
        StringBuilder target,
        SinkRecord record,
        JsonNode key,
        JsonNode value,
        boolean rawBytesUtf8) {
      target.append(text);
    }
  }

  private record Reference(
      String source, JsonPointer pointer, String name, boolean hasDefault, String defaultValue)
      implements Segment {
    @Override
    public void append(
        StringBuilder target,
        SinkRecord record,
        JsonNode key,
        JsonNode value,
        boolean rawBytesUtf8) {
      JsonNode resolved;
      try {
        if (pointer == null && source.equals("value") && isRawBytes(record.value())) {
          target.append(rawState(record.value(), rawBytesUtf8));
          return;
        }
        if (pointer == null && source.equals("key") && isRawBytes(record.key())) {
          target.append(rawState(record.key(), rawBytesUtf8));
          return;
        }
        resolved = resolve(record, key, value);
      } catch (InvalidHeaderUtf8 error) {
        if (hasDefault) {
          target.append(defaultValue);
          return;
        }
        throw new TemplateResolutionException("Template header is not valid UTF-8", error);
      }
      if (resolved == null || resolved.isMissingNode() || resolved.isNull()) {
        if (hasDefault) {
          target.append(defaultValue);
          return;
        }
        throw new TemplateResolutionException("Required template reference is missing");
      }
      target.append(renderValue(resolved));
    }

    private JsonNode resolve(SinkRecord record, JsonNode key, JsonNode value) {
      return switch (source) {
        case "key" -> pointer == null ? key : key.at(pointer);
        case "value" -> pointer == null ? value : value.at(pointer);
        case "header" -> header(record);
        case "metadata" -> metadata(record);
        default -> throw new IllegalStateException("Validated template source became invalid");
      };
    }

    private JsonNode header(SinkRecord record) {
      Header header = record.headers().lastWithName(name);
      if (header == null || header.value() == null) {
        return null;
      }
      Object headerValue = header.value();
      if (headerValue instanceof byte[] bytes) {
        return JsonNodeFactory.instance.textNode(decodeUtf8(ByteBuffer.wrap(bytes)));
      }
      if (headerValue instanceof ByteBuffer bytes) {
        return JsonNodeFactory.instance.textNode(decodeUtf8(bytes.duplicate()));
      }
      return new ConnectValueCanonicalizer().canonicalize(header.schema(), headerValue);
    }

    private JsonNode metadata(SinkRecord record) {
      return switch (name) {
        case "topic" -> JsonNodeFactory.instance.textNode(record.topic());
        case "partition" -> JsonNodeFactory.instance.numberNode(record.kafkaPartition());
        case "offset" -> JsonNodeFactory.instance.numberNode(record.kafkaOffset());
        case "timestamp" ->
            record.timestamp() == null
                ? JsonNodeFactory.instance.nullNode()
                : JsonNodeFactory.instance.numberNode(record.timestamp());
        default -> throw new IllegalStateException("Validated metadata name became invalid");
      };
    }

    private static String decodeUtf8(ByteBuffer bytes) {
      try {
        return StrictUtf8.decode(bytes);
      } catch (CharacterCodingException error) {
        throw new InvalidHeaderUtf8(error);
      }
    }

    private static boolean isRawBytes(Object value) {
      return value instanceof byte[] || value instanceof ByteBuffer;
    }

    private static String rawState(Object value, boolean rawBytesUtf8) {
      if (!rawBytesUtf8) {
        throw new PermanentRecordException(
            "STATE_BUILDING",
            "RAW_BYTES_DISABLED",
            "Raw byte Evaluation State requires state.raw_bytes.encoding=UTF-8");
      }
      return StrictUtf8.decodeRawState(value);
    }

    private static String renderValue(JsonNode node) {
      return node.isTextual() ? node.textValue() : CanonicalJson.serialize(node);
    }
  }

  private static final class InvalidHeaderUtf8 extends RuntimeException {
    private InvalidHeaderUtf8(Throwable cause) {
      super(cause);
    }
  }
}
