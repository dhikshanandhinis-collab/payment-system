#!/usr/bin/env bash
# Roll the production stack to a git ref and wait until the services are healthy.
# Usage (on the VM): bash deploy/deploy.sh [git-ref]   (CI passes the commit SHA)
set -euo pipefail
cd "$(dirname "$0")/.."

ref="${1:-origin/main}"
compose=(docker compose -f docker-compose.yml -f docker-compose.prod.yml --profile observability)

[ -f .env ] || { echo ".env is missing - copy .env.example and set real secrets" >&2; exit 1; }

git fetch --prune origin
git checkout --detach "$ref"
echo "deploying $(git rev-parse --short HEAD)"

"${compose[@]}" build
"${compose[@]}" up -d --remove-orphans

for svc in ingestion reconciliation batch-scheduler; do
  state=unknown
  for _ in $(seq 1 30); do
    id=$("${compose[@]}" ps -q "$svc")
    state=$(docker inspect -f '{{.State.Health.Status}}' "$id" 2>/dev/null || echo unknown)
    [ "$state" = healthy ] && break
    sleep 10
  done
  if [ "$state" != healthy ]; then
    echo "$svc is $state after 5 minutes" >&2
    "${compose[@]}" logs --tail 50 "$svc" >&2
    exit 1
  fi
  echo "$svc healthy"
done

docker image prune -f >/dev/null
echo "deploy complete"
