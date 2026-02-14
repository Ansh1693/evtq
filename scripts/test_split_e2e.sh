#!/usr/bin/env bash
set -euo pipefail

SQS_BASE_URL="${SQS_BASE_URL:-http://localhost:8080}"
FAAS_BASE_URL="${FAAS_BASE_URL:-http://localhost:8081}"
FN_DIR="${FN_DIR:-/tmp/faas-sample-fn}"

mkdir -p "$FN_DIR"
cat > "$FN_DIR/index.js" <<'EOF'
exports.handler = async (event) => {
  return { ok: true, count: (event.records || []).length };
};
EOF

echo "Creating function..."
curl -s -X POST "$FAAS_BASE_URL/functions" \
  -H 'Content-Type: application/json' \
  -d "{
    \"name\":\"hello-fn\",
    \"runtime\":\"nodejs22\",
    \"handler\":\"index.handler\",
    \"timeout_seconds\":30,
    \"memory_mb\":128,
    \"code_path\":\"$FN_DIR\",
    \"container_strategy\":\"warm\",
    \"warm_pool_size\":1
  }" >/dev/null

echo "Creating queue..."
curl -s -X POST "$SQS_BASE_URL/queues" \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders"}' >/dev/null

echo "Creating lambda trigger..."
trigger_id=$(
  curl -s -X POST "$SQS_BASE_URL/queues/orders/triggers" \
    -H 'Content-Type: application/json' \
    -d '{"target_type":"lambda","target_url":"hello-fn","batch_size":5,"enabled":true}' | \
    jq -r '.id'
)

echo "Sending message..."
curl -s -X POST "$SQS_BASE_URL/queues/orders/messages" \
  -H 'Content-Type: application/json' \
  -d '{"body":"hello split services"}' >/dev/null

sleep 3
echo "Trigger metrics:"
curl -s "$SQS_BASE_URL/queues/orders/triggers/$trigger_id/metrics" | jq .

echo "Queue stats:"
curl -s "$SQS_BASE_URL/queues/orders" | jq .

echo "Done."
