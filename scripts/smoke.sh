#!/usr/bin/env bash
# CI smoke test: runs the demo against a fresh deployment and asserts the
# exact outcome. Usage: scripts/smoke.sh [base url]
set -euo pipefail
URL=${1:-http://localhost:8080}
"$(dirname "$0")/demo.sh" "$URL"

stats=$(curl -sf "$URL/v1/stats")
check() { # check <jq expr> <want>
  got=$(jq -r "$1" <<<"$stats")
  [ "$got" = "$2" ] || { echo "FAIL: $1 = $got, want $2"; echo "$stats" | jq .; exit 1; }
}
check .events.processed 50
check .messages.delivered 130 # 40 order.created × 3 routes + 10 customer.updated × 1
check .ingest_duplicates 10

# The always-failing event exhausts its attempts and dead-letters.
for _ in $(seq 1 60); do
  stats=$(curl -sf "$URL/v1/stats")
  [ "$(jq .events.dead <<<"$stats")" = 1 ] && break
  sleep 1
done
check .events.dead 1
echo "smoke OK"
