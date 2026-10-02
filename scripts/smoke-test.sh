#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

compose="docker compose"
connect_port=${CONNECT_PORT:-18083}
connector_url=http://localhost:${connect_port}/connectors/jev-local

cleanup() {
  if [ "${KEEP_SMOKE_ENV:-0}" != "1" ]; then
    $compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

fail() {
  echo "smoke test failed: $*" >&2
  exit 1
}

wait_for_connect() {
  attempts=0
  until curl --fail --silent "http://localhost:${connect_port}/connector-plugins" >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    [ "$attempts" -lt 60 ] || fail "Kafka Connect REST API did not become ready"
    sleep 2
  done
}

wait_for_task_state() {
  expected=$1
  attempts=0
  while [ "$attempts" -lt 60 ]; do
    state=$(curl --silent "$connector_url/status" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("tasks", [{}])[0].get("state", ""))' 2>/dev/null || true)
    [ "$state" = "$expected" ] && return 0
    attempts=$((attempts + 1))
    sleep 1
  done
  fail "connector task did not reach $expected"
}

produce() {
  printf '%s\n' "$1" | $compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 --topic jev-input \
    --property parse.key=true --property 'key.separator=|'
}

consume_one() {
  topic=$1
  $compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic "$topic" --from-beginning \
    --max-messages 1 --timeout-ms 30000 2>/dev/null
}

committed_offset() {
  $compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 --describe --group connect-jev-local 2>/dev/null \
    | awk '$2 == "jev-input" && $3 == "0" { print $4 }'
}

wait_for_committed_offset() {
  expected=$1
  attempts=0
  while [ "$attempts" -lt 30 ]; do
    [ "$(committed_offset || true)" = "$expected" ] && return 0
    attempts=$((attempts + 1))
    sleep 1
  done
  fail "committed offset did not reach $expected"
}

mvn -q -DskipTests package
$compose down --volumes --remove-orphans >/dev/null 2>&1 || true
$compose up --build -d
wait_for_connect

for topic in jev-input jev-output jev-dlq; do
  $compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --create --if-not-exists \
    --partitions 1 --replication-factor 1 --topic "$topic" >/dev/null
done

curl --fail --silent --show-error -X POST "http://localhost:${connect_port}/connectors" \
  -H 'Content-Type: application/json' --data @config/local-connector.json >/dev/null
wait_for_task_state RUNNING

produce 'customer-42|{"message":"happy-source-secret"}'
enriched=$(consume_one jev-output)
python3 -c '
import json,sys
d=json.loads(sys.argv[1])
assert d["source"]["topic"] == "jev-input"
assert d["source"]["key"] == "customer-42"
assert d["input"] == {"message":"happy-source-secret"}
assert d["evaluation"]["question_set"]["id"] == "support-routing/local-v1"
assert d["evaluation"]["model"] == {"requested":"jev-test-1","resolved":"jev-test-1"}
assert d["jev"] == {"model":"jev-test-1","answers":{"department":"technical"},"usage":{"input_tokens":7,"output_tokens":1},"fake":{"deterministic":True}}
' "$enriched" || fail "Enriched Record did not satisfy the v1 contract"
echo ENRICHED_RECORD_OK

produce 'customer-43|{"message":"CONTROL_PERMANENT"}'
dead_letter=$(consume_one jev-dlq)
python3 -c '
import json,sys
d=json.loads(sys.argv[1])
assert d["source"]["key"] == "customer-43"
assert d["error"] == {"category":"JEV_REQUEST","code":"RECORD_TOO_LARGE","message":"Jev rejected the Evaluation State as too large"}
assert "state" in d["evaluation"]
serialized=json.dumps(d)
for forbidden in ("permanent-secret-marker", "questions", "Authorization", "local-fake-jev-key"):
    assert forbidden not in serialized
' "$dead_letter" || fail "Dead-Letter Record was not sanitized"
echo SANITIZED_DEAD_LETTER_OK

wait_for_committed_offset 2
before=$(committed_offset)
produce 'customer-44|{"message":"CONTROL_TRANSIENT"}'
wait_for_task_state FAILED
sleep 2
after=$(committed_offset)
[ "$before" = "2" ] || fail "unexpected pre-failure committed offset: $before"
[ "$after" = "$before" ] || fail "transiently failed Source Record was committed ($before -> $after)"
echo TRANSIENT_UNCOMMITTED_OK

logs=$($compose logs --no-color connect fake-jev 2>&1)
for forbidden in local-fake-jev-key happy-source-secret permanent-secret-marker transient-secret-marker; do
  if printf '%s' "$logs" | grep -F "$forbidden" >/dev/null; then
    fail "logs exposed forbidden marker: $forbidden"
  fi
done
echo LOG_BOUNDARY_OK
