package io.github.smatiolids.kafkajev;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;

/** A compiled, finite policy for selecting one successful publication topic. */
final class DestinationPolicy {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String QUESTION = "([A-Za-z_][A-Za-z0-9_-]*)";
  private static final String NUMBER = "([-+]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)(?:[eE][-+]?\\d+)?)";
  private static final String STRING = "(\"(?:\\\\.|[^\"\\\\])*\")";
  private static final Pattern DIRECT =
      Pattern.compile("answers\\." + QUESTION + "\\.(choice|level)");
  private static final Pattern CONDITIONAL =
      Pattern.compile(
          "answers\\."
              + QUESTION
              + "\\.(noul|score)\\s*(>=|<=|>|<)\\s*"
              + NUMBER
              + "\\s*;\\s*"
              + STRING
              + "\\s*,\\s*"
              + STRING);
  private static final Pattern TOPIC = Pattern.compile("[A-Za-z0-9._-]+");

  private final String prefix;
  private final String suffix;
  private final Kind kind;
  private final String question;
  private final String expectedAnswerType;
  private final List<String> configuredValues;
  private final Operator operator;
  private final BigDecimal threshold;
  private final BigDecimal numericMaximum;
  private final String trueFragment;
  private final String falseFragment;
  private final Set<String> candidates;

  private DestinationPolicy(
      String prefix,
      String suffix,
      Kind kind,
      String question,
      String expectedAnswerType,
      List<String> configuredValues,
      Operator operator,
      BigDecimal threshold,
      BigDecimal numericMaximum,
      String trueFragment,
      String falseFragment,
      List<String> candidateFragments) {
    this.prefix = prefix;
    this.suffix = suffix;
    this.kind = kind;
    this.question = question;
    this.expectedAnswerType = expectedAnswerType;
    this.configuredValues = List.copyOf(configuredValues);
    this.operator = operator;
    this.threshold = threshold;
    this.numericMaximum = numericMaximum;
    this.trueFragment = trueFragment;
    this.falseFragment = falseFragment;

    LinkedHashSet<String> rendered = new LinkedHashSet<>();
    for (String fragment : candidateFragments) {
      String topic = prefix + fragment + suffix;
      validateTopic(topic);
      rendered.add(topic);
    }
    this.candidates = Set.copyOf(rendered);
  }

  static DestinationPolicy compile(String configured, JsonNode questions) {
    int opening = configured.indexOf("${");
    if (opening < 0) {
      return new DestinationPolicy(
          configured,
          "",
          Kind.LITERAL,
          null,
          null,
          List.of(),
          null,
          null,
          null,
          null,
          null,
          List.of(""));
    }
    int closing = configured.indexOf('}', opening + 2);
    if (closing < 0
        || configured.indexOf("${", opening + 2) >= 0
        || configured.indexOf('}', closing + 1) >= 0) {
      throw invalidPolicy("must contain exactly one well-formed Routing Expression");
    }

    String prefix = configured.substring(0, opening);
    String expression = configured.substring(opening + 2, closing);
    String suffix = configured.substring(closing + 1);
    Matcher direct = DIRECT.matcher(expression);
    if (direct.matches()) {
      String question = direct.group(1);
      String selector = direct.group(2);
      JsonNode definition = question(questions, question);
      if ("choice".equals(selector)) {
        requireType(definition, question, "choice");
        JsonNode criteria = definition.get("criteria");
        if (criteria == null || !criteria.isObject() || criteria.isEmpty()) {
          throw invalidPolicy("Choice question must define nonempty object criteria");
        }
        List<String> choices = new ArrayList<>();
        criteria.fieldNames().forEachRemaining(choices::add);
        return new DestinationPolicy(
            prefix,
            suffix,
            Kind.CHOICE,
            question,
            "choice",
            choices,
            null,
            null,
            null,
            null,
            null,
            choices);
      }

      requireType(definition, question, "score");
      int levels = scoreLevels(definition);
      List<String> levelNames = new ArrayList<>(levels);
      for (int level = 0; level < levels; level++) {
        levelNames.add(Integer.toString(level));
      }
      return new DestinationPolicy(
          prefix,
          suffix,
          Kind.LEVEL,
          question,
          "score",
          levelNames,
          null,
          null,
          null,
          null,
          null,
          levelNames);
    }

    Matcher conditional = CONDITIONAL.matcher(expression);
    if (!conditional.matches()) {
      throw invalidPolicy("contains an unsupported or malformed Routing Expression");
    }
    String question = conditional.group(1);
    String selector = conditional.group(2);
    String expectedType = "noul".equals(selector) ? "noul" : "score";
    JsonNode definition = question(questions, question);
    requireType(definition, question, expectedType);
    BigDecimal maximum =
        "noul".equals(selector)
            ? BigDecimal.ONE
            : BigDecimal.valueOf(scoreLevels(definition) - 1L);
    BigDecimal threshold;
    try {
      threshold = new BigDecimal(conditional.group(4));
    } catch (NumberFormatException invalidNumber) {
      throw invalidPolicy("numeric threshold must be finite");
    }
    Operator operator = Operator.parse(conditional.group(3));
    if (!operator.bothBranchesReachable(BigDecimal.ZERO, maximum, threshold)) {
      throw invalidPolicy("numeric threshold must make both branches reachable");
    }
    String whenTrue = stringLiteral(conditional.group(5));
    String whenFalse = stringLiteral(conditional.group(6));
    return new DestinationPolicy(
        prefix,
        suffix,
        "noul".equals(selector) ? Kind.NOUL : Kind.SCORE,
        question,
        expectedType,
        List.of(),
        operator,
        threshold,
        maximum,
        whenTrue,
        whenFalse,
        List.of(whenTrue, whenFalse));
  }

  Set<String> candidates() {
    return candidates;
  }

  String resolve(InferenceResult result) {
    if (kind == Kind.LITERAL) {
      return candidates.iterator().next();
    }
    JsonNode answers = result.json().get("answers");
    JsonNode answer = answers != null && answers.isObject() ? answers.get(question) : null;
    if (answer == null || !answer.isObject()) {
      throw contractViolation("missing or malformed Routing Answer");
    }
    JsonNode type = answer.get("type");
    if (type == null || !type.isTextual() || !expectedAnswerType.equals(type.textValue())) {
      throw contractViolation("Routing Answer type does not match its Question");
    }

    String fragment =
        switch (kind) {
          case CHOICE -> choice(answer);
          case LEVEL -> level(answer);
          case NOUL -> branch(answer, "noul", BigDecimal.ZERO, BigDecimal.ONE);
          case SCORE ->
              branch(answer, "score", BigDecimal.ZERO, numericMaximum);
          case LITERAL -> throw new IllegalStateException("literal policy already resolved");
        };
    String rendered = prefix + fragment + suffix;
    if (!candidates.contains(rendered)) {
      throw contractViolation("Routing Answer selected an unconfigured Destination Topic");
    }
    return rendered;
  }

  private String choice(JsonNode answer) {
    JsonNode choice = answer.get("choice");
    if (choice == null || !choice.isTextual() || !configuredValues.contains(choice.textValue())) {
      throw contractViolation("Choice answer is absent or is not a configured criterion");
    }
    return choice.textValue();
  }

  private String level(JsonNode answer) {
    JsonNode probabilities = answer.get("probabilities");
    if (probabilities == null
        || !probabilities.isObject()
        || probabilities.size() != configuredValues.size()) {
      throw contractViolation("Score probabilities do not match the configured levels");
    }
    int selected = -1;
    BigDecimal greatest = null;
    for (String level : configuredValues) {
      JsonNode probability = probabilities.get(level);
      BigDecimal value = numeric(probability, "Score probability");
      if (value.compareTo(BigDecimal.ZERO) < 0 || value.compareTo(BigDecimal.ONE) > 0) {
        throw contractViolation("Score probability is outside zero through one");
      }
      int index = Integer.parseInt(level);
      if (greatest == null || value.compareTo(greatest) >= 0) {
        greatest = value;
        selected = index;
      }
    }
    probabilities.fieldNames().forEachRemaining(
        field -> {
          if (!configuredValues.contains(field)) {
            throw contractViolation("Score probabilities contain an unexpected level");
          }
        });
    return Integer.toString(selected);
  }

  private String branch(JsonNode answer, String field, BigDecimal minimum, BigDecimal maximum) {
    BigDecimal value = numeric(answer.get(field), field + " value");
    if (value.compareTo(minimum) < 0 || value.compareTo(maximum) > 0) {
      throw contractViolation(field + " value is outside its configured domain");
    }
    return operator.test(value, threshold) ? trueFragment : falseFragment;
  }

  private static BigDecimal numeric(JsonNode value, String field) {
    if (value == null || !value.isNumber() || !Double.isFinite(value.doubleValue())) {
      throw contractViolation(field + " must be a finite number");
    }
    return value.decimalValue();
  }

  private static JsonNode question(JsonNode questions, String name) {
    JsonNode definition = questions.get(name);
    if (definition == null || !definition.isObject()) {
      throw invalidPolicy("referenced Routing Question does not exist");
    }
    return definition;
  }

  private static void requireType(JsonNode definition, String question, String expected) {
    JsonNode type = definition.get("type");
    if (type == null || !type.isTextual() || !expected.equals(type.textValue())) {
      throw invalidPolicy("selector does not match the type of question " + question);
    }
  }

  private static int scoreLevels(JsonNode definition) {
    JsonNode criteria = definition.get("criteria");
    if (criteria == null || !criteria.isArray() || criteria.isEmpty()) {
      throw invalidPolicy("Score question must define a nonempty ordered criteria array");
    }
    return criteria.size();
  }

  private static String stringLiteral(String source) {
    try {
      JsonNode value = JSON.readTree(source);
      if (value != null && value.isTextual()) {
        return value.textValue();
      }
    } catch (Exception ignored) {
      // The public error below intentionally does not expose parser internals.
    }
    throw invalidPolicy("conditional branches must be valid quoted string fragments");
  }

  private static void validateTopic(String topic) {
    if (topic.isEmpty()
        || topic.length() > 249
        || ".".equals(topic)
        || "..".equals(topic)
        || !TOPIC.matcher(topic).matches()) {
      throw invalidPolicy("renders an invalid Kafka Destination Topic");
    }
  }

  private static ConfigException invalidPolicy(String reason) {
    return new ConfigException(JevConnectorConfig.OUTPUT_TOPIC, null, reason);
  }

  private static RoutingContractViolation contractViolation(String reason) {
    return new RoutingContractViolation("Jev response violates Destination Policy: " + reason);
  }

  private enum Kind {
    LITERAL,
    CHOICE,
    LEVEL,
    NOUL,
    SCORE
  }

  private enum Operator {
    GREATER,
    GREATER_OR_EQUAL,
    LESS,
    LESS_OR_EQUAL;

    static Operator parse(String value) {
      return switch (value) {
        case ">" -> GREATER;
        case ">=" -> GREATER_OR_EQUAL;
        case "<" -> LESS;
        case "<=" -> LESS_OR_EQUAL;
        default -> throw invalidPolicy("unsupported numeric operator");
      };
    }

    boolean test(BigDecimal left, BigDecimal right) {
      int comparison = left.compareTo(right);
      return switch (this) {
        case GREATER -> comparison > 0;
        case GREATER_OR_EQUAL -> comparison >= 0;
        case LESS -> comparison < 0;
        case LESS_OR_EQUAL -> comparison <= 0;
      };
    }

    boolean bothBranchesReachable(BigDecimal minimum, BigDecimal maximum, BigDecimal value) {
      boolean minimumResult = test(minimum, value);
      boolean maximumResult = test(maximum, value);
      return minimumResult != maximumResult;
    }
  }

  static final class RoutingContractViolation extends ConnectException {
    RoutingContractViolation(String message) {
      super(message);
    }
  }
}
