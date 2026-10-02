package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.apache.kafka.connect.errors.ConnectException;
import org.erdtman.jcs.JsonCanonicalizer;

final class CanonicalJson {
  static final ObjectMapper MAPPER = new ObjectMapper();

  private CanonicalJson() {}

  static String serialize(JsonNode value) {
    try {
      if (value.isObject()) {
        return new JsonCanonicalizer(MAPPER.writeValueAsBytes(value)).getEncodedString();
      }
      ObjectNode wrapper = MAPPER.createObjectNode();
      wrapper.set("v", value);
      String canonicalWrapper =
          new JsonCanonicalizer(MAPPER.writeValueAsBytes(wrapper)).getEncodedString();
      return canonicalWrapper.substring("{\"v\":".length(), canonicalWrapper.length() - 1);
    } catch (Exception error) {
      throw new ConnectException("Could not canonicalize JSON", error);
    }
  }

  static String write(JsonNode value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (Exception error) {
      throw new ConnectException("Could not serialize JSON", error);
    }
  }

  static String hash(JsonNode value) {
    return hashText(serialize(value));
  }

  static String hashText(String value) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      return "sha256:" + HexFormat.of().formatHex(digest);
    } catch (Exception error) {
      throw new IllegalStateException("SHA-256 is unavailable", error);
    }
  }

  static ObjectNode object(Object... values) {
    ObjectNode object = MAPPER.createObjectNode();
    for (int index = 0; index < values.length; index += 2) {
      String name = (String) values[index];
      Object value = values[index + 1];
      if (value == null) {
        object.putNull(name);
      } else if (value instanceof JsonNode node) {
        object.set(name, node);
      } else if (value instanceof Integer integer) {
        object.put(name, integer);
      } else if (value instanceof Long longValue) {
        object.put(name, longValue);
      } else if (value instanceof Boolean booleanValue) {
        object.put(name, booleanValue);
      } else {
        object.put(name, String.valueOf(value));
      }
    }
    return object;
  }
}
