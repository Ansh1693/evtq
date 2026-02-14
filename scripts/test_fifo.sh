#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
QUEUE="${QUEUE:-orders.fifo}"

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1"; exit 1; }
}

require_cmd curl
require_cmd jq

curl_json() {
  curl -sS -f "$@"
}

create_queue() {
  echo "==> Creating queue: $QUEUE"
  curl_json -X POST "$BASE_URL/queues" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$QUEUE\",\"queue_type\":\"FIFO\",\"content_based_dedup\":false}" | jq .
}

purge_queue() {
  echo "==> Purging queue"
  curl_json -X DELETE "$BASE_URL/queues/$QUEUE/messages" | jq .
}

delete_queue() {
  curl -sS -X DELETE "$BASE_URL/queues/$QUEUE" -o /dev/null -w "%{http_code}"
}

ensure_fifo_queue() {
  local status body queue_type
  status="$(curl -sS -o /tmp/fifo_queue_get.json -w "%{http_code}" "$BASE_URL/queues/$QUEUE")"
  if [[ "$status" == "200" ]]; then
    body="$(cat /tmp/fifo_queue_get.json)"
    queue_type="$(echo "$body" | jq -r '.queue.queue_type // "STANDARD"')"
    if [[ "$queue_type" != "FIFO" ]]; then
      echo "==> Existing queue is not FIFO (queue_type=$queue_type), recreating"
      code="$(delete_queue)"
      [[ "$code" == "204" || "$code" == "404" ]] || { echo "FAILED: delete queue returned $code"; exit 1; }
      create_queue >/dev/null
    fi
  elif [[ "$status" == "404" ]]; then
    create_queue >/dev/null
  else
    echo "FAILED: queue lookup returned HTTP $status"
    cat /tmp/fifo_queue_get.json
    exit 1
  fi
}

send_msg() {
  local body="$1" group="$2" dedup="$3"
  curl_json -X POST "$BASE_URL/queues/$QUEUE/messages" \
    -H "Content-Type: application/json" \
    -d "{\"body\":\"$body\",\"message_group_id\":\"$group\",\"message_dedup_id\":\"$dedup\"}"
}

recv_msg() {
  local wait="${1:-0}" max="${2:-1}"
  curl_json -X POST \
    "$BASE_URL/queues/$QUEUE/messages/receive?wait_time_seconds=$wait&max_messages=$max"
}

delete_msg() {
  local rh="$1"
  curl -sS -X DELETE "$BASE_URL/queues/$QUEUE/messages/$rh" -o /dev/null -w "%{http_code}"
}

change_vis() {
  local rh="$1" timeout="$2"
  curl -sS -X PUT "$BASE_URL/queues/$QUEUE/messages/$rh/visibility" \
    -H "Content-Type: application/json" \
    -d "{\"visibility_timeout\":$timeout}" -o /dev/null -w "%{http_code}"
}

echo "Using BASE_URL=$BASE_URL QUEUE=$QUEUE"
ensure_fifo_queue
purge_queue >/dev/null

echo
echo "TEST 1: Strict ordering within one group"
purge_queue >/dev/null
send_msg "1" "A" "A-1" >/dev/null
send_msg "2" "A" "A-2" >/dev/null
send_msg "3" "A" "A-3" >/dev/null

for expected in 1 2 3; do
  out="$(recv_msg 0 1)"
  body="$(echo "$out" | jq -r '.messages[0].body')"
  rh="$(echo "$out" | jq -r '.messages[0].receipt_handle')"
  seq="$(echo "$out" | jq -r '.messages[0].sequence_number')"
  echo "  got body=$body seq=$seq"
  [[ "$body" == "$expected" ]] || { echo "FAILED: expected $expected, got $body"; exit 1; }
  code="$(delete_msg "$rh")"
  [[ "$code" == "204" ]] || { echo "FAILED: delete returned $code"; exit 1; }
done
echo "  PASS"

echo
echo "TEST 2: Group blocking"
purge_queue >/dev/null
send_msg "block-1" "B" "B-1" >/dev/null
first="$(recv_msg 0 1)"
first_body="$(echo "$first" | jq -r '.messages[0].body')"
first_rh="$(echo "$first" | jq -r '.messages[0].receipt_handle')"
echo "  first receive body=$first_body"
second="$(recv_msg 0 1)"
second_count="$(echo "$second" | jq '.messages | length')"
[[ "$second_count" == "0" ]] || { echo "FAILED: expected blocked group empty receive"; exit 1; }
delete_msg "$first_rh" >/dev/null
echo "  PASS"

echo
echo "TEST 3: Cross-group parallelism"
purge_queue >/dev/null
send_msg "ga-1" "GA" "GA-1" >/dev/null
send_msg "gb-1" "GB" "GB-1" >/dev/null
batch="$(recv_msg 0 10)"
count="$(echo "$batch" | jq '.messages | length')"
echo "  received count=$count"
[[ "$count" -ge 2 ]] || { echo "FAILED: expected >=2 messages across groups"; exit 1; }
echo "$batch" | jq -r '.messages[].receipt_handle' | while read -r rh; do delete_msg "$rh" >/dev/null; done
echo "  PASS"

echo
echo "TEST 5: Deduplication"
purge_queue >/dev/null
first="$(send_msg "dedup-body" "D" "XYZ")"
id1="$(echo "$first" | jq -r '.id')"
seq1="$(echo "$first" | jq -r '.sequence_number')"
second="$(send_msg "dedup-body-different-content-ok" "D" "XYZ")"
id2="$(echo "$second" | jq -r '.id')"
seq2="$(echo "$second" | jq -r '.sequence_number')"
echo "  first id=$id1 seq=$seq1"
echo "  second id=$id2 seq=$seq2"
[[ "$id1" == "$id2" ]] || { echo "FAILED: dedup did not return original ID"; exit 1; }
echo "  PASS"

echo
echo "TEST 6: Visibility timeout reorder safety"
purge_queue >/dev/null
send_msg "A" "X" "X-A1" >/dev/null
send_msg "B" "X" "X-B1" >/dev/null
m1="$(recv_msg 0 1)"
b1="$(echo "$m1" | jq -r '.messages[0].body')"
rh1="$(echo "$m1" | jq -r '.messages[0].receipt_handle')"
[[ "$b1" == "A" ]] || { echo "FAILED: expected A first"; exit 1; }
echo "  got first=$b1, forcing visibility=0"
change_vis "$rh1" 0 >/dev/null
m2="$(recv_msg 0 1)"
b2="$(echo "$m2" | jq -r '.messages[0].body')"
rh2="$(echo "$m2" | jq -r '.messages[0].receipt_handle')"
[[ "$b2" == "A" ]] || { echo "FAILED: expected A again after visibility reset"; exit 1; }
echo "  PASS"

echo
echo "TEST 7: Old receipt handle invalid after re-receive"
code_old="$(delete_msg "$rh1")"
[[ "$code_old" != "204" ]] || { echo "FAILED: old receipt handle should be stale"; exit 1; }
code_new="$(delete_msg "$rh2")"
[[ "$code_new" == "204" ]] || { echo "FAILED: new receipt handle should work"; exit 1; }
echo "  PASS"

echo
echo "All FIFO tests passed."