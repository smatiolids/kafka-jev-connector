package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.errors.ConnectException;

record InferenceResult(ObjectNode json, String resolvedModel) {
  private static final String INVALID_RESPONSE =
      "Jev response must be a JSON object with a usable model";

  static InferenceResult from(JsonNode response) {
    if (!(response instanceof ObjectNode object)) {
      throw new ConnectException(INVALID_RESPONSE);
    }
    JsonNode model = object.get("model");
    if (model == null || !model.isTextual()) {
      throw new ConnectException(INVALID_RESPONSE);
    }
    String resolvedModel = model.textValue();
    if (resolvedModel.isBlank() || !resolvedModel.equals(resolvedModel.strip())) {
      throw new ConnectException(INVALID_RESPONSE);
    }
    return new InferenceResult(object, resolvedModel);
  }
}
