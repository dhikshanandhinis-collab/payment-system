#!/usr/bin/env bash
# Create a production .env from .env.example: random DB/Grafana passwords plus the given SMTP settings.
# Usage: init_env.sh <file-with-SMTP_*-and-ALERT_EMAIL_TO-lines> <target-.env>
set -euo pipefail
smtp_file="$1"
target="$2"
src="$(cd "$(dirname "$0")/.." && pwd)/.env.example"

if [ -f "$target" ]; then
  echo "$target already exists; leaving it unchanged"
  exit 0
fi

rand() { openssl rand -hex 24; }
declare -A vals=(
  [ENVIRONMENT]=production
  [SIM_RUN_ID]=prod
  [SIM_PAYMENTS_PER_SECOND]=0.5
  [DB_SUPERUSER_PASSWORD]=$(rand)
  [DB_MIGRATOR_PASSWORD]=$(rand)
  [DB_PASSWORD]=$(rand)
  [DB_REPORTING_PASSWORD]=$(rand)
  [GRAFANA_ADMIN_PASSWORD]=$(rand)
)
while IFS='=' read -r key value; do
  vals[$key]=$value
done < <(grep -E '^[A-Z_]+=' "$smtp_file")

umask 077
while IFS= read -r line; do
  key="${line%%=*}"
  if [[ "$line" =~ ^[A-Z_]+= ]] && [[ -v "vals[$key]" ]]; then
    printf '%s=%s\n' "$key" "${vals[$key]}"
  else
    printf '%s\n' "$line"
  fi
done < "$src" > "$target"
echo "wrote $target"
