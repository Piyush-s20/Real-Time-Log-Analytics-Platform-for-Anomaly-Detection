#!/usr/bin/env bash
# Imports the data view, visualizations, saved search and dashboard from
# kibana/dashboard.ndjson through Kibana's saved objects import API.
#
# Usage: ./scripts/import_kibana.sh [KIBANA_URL]   (default http://localhost:5601)
set -euo pipefail

KIBANA_URL="${1:-${KIBANA_URL:-http://localhost:5601}}"
NDJSON="$(cd "$(dirname "$0")/.." && pwd)/kibana/dashboard.ndjson"

echo "Waiting for Kibana at $KIBANA_URL ..."
until curl -s "$KIBANA_URL/api/status" | grep -q '"level":"available"'; do
  sleep 5
done

echo "Importing $NDJSON ..."
response="$(curl -s -X POST "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  --form file=@"$NDJSON")"
echo "$response"

if echo "$response" | grep -q '"success":true'; then
  echo "Done. Open $KIBANA_URL/app/dashboards#/view/log-analytics-dashboard"
else
  echo "Import failed." >&2
  exit 1
fi
