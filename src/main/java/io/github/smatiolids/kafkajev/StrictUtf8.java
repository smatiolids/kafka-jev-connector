package io.github.smatiolids.kafkajev;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

final class StrictUtf8 {
  private StrictUtf8() {}

  static String decode(ByteBuffer bytes) throws CharacterCodingException {
    return StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(bytes.duplicate())
        .toString();
  }

  static String decodeRawState(Object value) {
    ByteBuffer bytes =
        value instanceof byte[] array ? ByteBuffer.wrap(array) : ((ByteBuffer) value).duplicate();
    try {
      return decode(bytes);
    } catch (CharacterCodingException error) {
      throw new PermanentRecordException(
          "STATE_BUILDING", "INVALID_UTF8", "Raw byte Evaluation State is not valid UTF-8");
    }
  }
}
