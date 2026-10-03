package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

final class ConnectValueCanonicalizer {
  private static final JsonNodeFactory JSON = JsonNodeFactory.instance;

  JsonNode canonicalize(Schema schema, Object value) {
    if (value == null) {
      return JSON.nullNode();
    }
    if (schema != null && schema.name() != null) {
      if (Decimal.LOGICAL_NAME.equals(schema.name())) {
        return JSON.textNode(((BigDecimal) value).toPlainString());
      }
      if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
        return JSON.textNode(
            Instant.ofEpochMilli(((Date) value).getTime())
                .atZone(ZoneOffset.UTC)
                .toLocalDate()
                .toString());
      }
      if (org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(schema.name())) {
        return JSON.textNode(
            DateTimeFormatter.ISO_LOCAL_TIME.format(
                Instant.ofEpochMilli(((Date) value).getTime())
                    .atZone(ZoneOffset.UTC)
                    .toLocalTime()));
      }
      if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(schema.name())) {
        return JSON.textNode(DateTimeFormatter.ISO_INSTANT.format(((Date) value).toInstant()));
      }
    }
    return schema == null ? schemaless(value) : schemaful(schema, value);
  }

  private JsonNode schemaful(Schema schema, Object value) {
    return switch (schema.type()) {
      case INT8 -> JSON.numberNode((Byte) value);
      case INT16 -> JSON.numberNode((Short) value);
      case INT32 -> JSON.numberNode((Integer) value);
      case INT64 -> JSON.numberNode((Long) value);
      case FLOAT32 -> finite((Float) value);
      case FLOAT64 -> finite((Double) value);
      case BOOLEAN -> JSON.booleanNode((Boolean) value);
      case STRING -> JSON.textNode((String) value);
      case BYTES -> bytes(value);
      case ARRAY -> array(schema.valueSchema(), (List<?>) value);
      case MAP -> map(schema.keySchema(), schema.valueSchema(), (Map<?, ?>) value);
      case STRUCT -> struct((Struct) value);
    };
  }

  private JsonNode schemaless(Object value) {
    if (value instanceof String string) return JSON.textNode(string);
    if (value instanceof Boolean booleanValue) return JSON.booleanNode(booleanValue);
    if (value instanceof Byte byteValue) return JSON.numberNode(byteValue);
    if (value instanceof Short shortValue) return JSON.numberNode(shortValue);
    if (value instanceof Integer integer) return JSON.numberNode(integer);
    if (value instanceof Long longValue) return JSON.numberNode(longValue);
    if (value instanceof Float floatValue) return finite(floatValue);
    if (value instanceof Double doubleValue) return finite(doubleValue);
    if (value instanceof BigInteger integer) return JSON.numberNode(integer);
    if (value instanceof BigDecimal decimal) return JSON.numberNode(decimal);
    if (value instanceof byte[] || value instanceof ByteBuffer) return bytes(value);
    if (value instanceof List<?> list) return array(null, list);
    if (value instanceof Map<?, ?> map) return map(null, null, map);
    if (value instanceof Struct struct) return struct(struct);
    throw new DataException("Unsupported Kafka Connect value type: " + value.getClass().getName());
  }

  private JsonNode finite(double value) {
    if (!Double.isFinite(value)) {
      throw new DataException("Non-finite floating-point values cannot be represented as JSON");
    }
    return JSON.numberNode(value);
  }

  private JsonNode finite(float value) {
    if (!Float.isFinite(value)) {
      throw new DataException("Non-finite floating-point values cannot be represented as JSON");
    }
    return JSON.numberNode(value);
  }

  private ObjectNode bytes(Object value) {
    byte[] content;
    if (value instanceof byte[] array) {
      content = array;
    } else if (value instanceof ByteBuffer buffer) {
      ByteBuffer copy = buffer.duplicate();
      content = new byte[copy.remaining()];
      copy.get(content);
    } else {
      throw new DataException("Kafka Connect BYTES value must be byte[] or ByteBuffer");
    }
    return CanonicalJson.object(
        "$type", "bytes", "base64", Base64.getEncoder().encodeToString(content));
  }

  private ArrayNode array(Schema elementSchema, List<?> values) {
    ArrayNode result = JSON.arrayNode();
    values.forEach(value -> result.add(canonicalize(elementSchema, value)));
    return result;
  }

  private JsonNode map(Schema keySchema, Schema valueSchema, Map<?, ?> values) {
    boolean stringKeys =
        keySchema != null
            ? keySchema.type() == Schema.Type.STRING
            : values.keySet().stream().allMatch(String.class::isInstance);
    if (stringKeys) {
      ObjectNode result = JSON.objectNode();
      values.forEach(
          (key, value) -> result.set((String) key, canonicalize(valueSchema, value)));
      return result;
    }
    List<JsonNode> sortedEntries = new ArrayList<>();
    values.forEach(
        (key, value) ->
            sortedEntries.add(
                CanonicalJson.object(
                    "key", canonicalize(keySchema, key),
                    "value", canonicalize(valueSchema, value))));
    sortedEntries.sort(
        Comparator.comparing((JsonNode entry) -> CanonicalJson.serialize(entry.path("key")))
            .thenComparing(entry -> CanonicalJson.serialize(entry.path("value"))));
    ArrayNode entries = JSON.arrayNode();
    sortedEntries.forEach(entries::add);
    return CanonicalJson.object("$type", "map", "entries", entries);
  }

  private ObjectNode struct(Struct struct) {
    ObjectNode result = JSON.objectNode();
    struct.schema().fields().forEach(
        field -> result.set(field.name(), canonicalize(field.schema(), struct.get(field))));
    return result;
  }
}
