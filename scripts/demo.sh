#!/usr/bin/env bash
# Drives a running hookgate (Go or NestJS) end to end:
#   1. registers a source and three routes
#   2. sends signed webhooks, re-sends some (dedup) and one that always fails
#   3. waits for the pipeline, then prints stats and one event's timeline
# Usage: scripts/demo.sh [base url]   (default http://localhost:8080; needs curl, jq, openssl)
set -euo pipefail
URL=${1:-http://localhost:8080}
SRC=${SRC:-shop}
AUTH=()
[ -n "${API_KEY:-}" ] && AUTH=(-H "Authorization: Bearer $API_KEY")

api() { curl -sS "${AUTH[@]}" -H 'Content-Type: application/json' "$@"; }

send() { # send <webhook id> <json body> -> HTTP status
  local t sig
  t=$(date +%s)
  sig=$(printf '%s.%s' "$t" "$2" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')
  curl -s -o /dev/null -w '%{http_code}' -X POST "$URL/v1/ingest/$SRC" \
    -H 'Content-Type: application/json' -H "Webhook-Id: $1" -H "Hookgate-Signature: t=$t,v1=$sig" -d "$2"
}

SECRET=${SECRET:-whsec_demo_secret}
api -X POST "$URL/v1/sources" -d "{\"name\":\"$SRC\",\"secret\":\"$SECRET\"}" >/dev/null # 409 on re-runs is fine
for r in 'order.*:billing' 'order.created:crm' '*:audit'; do
  api -X POST "$URL/v1/routes" -d "{\"source\":\"$SRC\",\"event_type\":\"${r%%:*}\",\"topic\":\"${r##*:}\"}" >/dev/null
done

run=$(date +%s)
echo "→ sending 50 webhooks (+10 provider retries, +1 that always fails)"
for i in $(seq 1 50); do
  type=order.created; (( i % 5 == 0 )) && type=customer.updated
  send "evt_${run}_$i" "{\"type\":\"$type\",\"order\":$i}" >/dev/null
done
for i in $(seq 1 10); do
  code=$(send "evt_${run}_$i" "{\"type\":\"order.created\",\"order\":$i}")
  [ "$code" = 200 ] || echo "  unexpected status for a duplicate: $code"
done
FLAKY=$(send "evt_${run}_flaky" '{"type":"payment.flaky"}')
echo "  duplicates answered 200 (already received); flaky event: $FLAKY"

echo "→ waiting for the pipeline to drain"
for _ in $(seq 1 60); do
  s=$(api "$URL/v1/stats")
  [ "$(jq '.outbox.ready' <<<"$s")" = 0 ] && [ "$(jq '.messages.pending + .messages.published' <<<"$s")" = 0 ] && break
  sleep 1
done
jq . <<<"$s"

id=$(api "$URL/v1/events?source=$SRC&type=order.created&limit=1" | jq -r '.events[0].id')
echo "→ timeline of event $id"
api "$URL/v1/events/$id" | jq -r '.timeline[] | "  \(.at[11:23])  \(.status|.+" "*(18-length))  \(.actor|.+" "*(15-length))  \(.detail // "")"'
echo "  messages:"
api "$URL/v1/events/$id" | jq -r '.messages[] | "    \(.topic): \(.status)"'
echo "→ the always-failing event retries with backoff, then dead-letters; watch it with:"
echo "  curl -s '$URL/v1/events?status=dead' | jq"
