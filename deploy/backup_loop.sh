#!/bin/sh
# Nightly logical backup of the recon database with N-day retention; runs inside postgres:17-alpine.
set -eu
interval="${BACKUP_INTERVAL_S:-86400}"
keep_days="${BACKUP_KEEP_DAYS:-7}"

while true; do
  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  target="/backups/${PGDATABASE}_${stamp}.dump"
  if pg_dump --format=custom --file="${target}.partial" && mv "${target}.partial" "$target"; then
    echo "backup ok: $target ($(du -h "$target" | cut -f1))"
    printf 'recon_backup_last_success_timestamp_seconds %s\n' "$(date +%s)" > /textfile/backup.prom.tmp
    mv /textfile/backup.prom.tmp /textfile/backup.prom
  else
    echo "backup FAILED: $target" >&2
    rm -f "${target}.partial"
  fi
  find /backups -name '*.dump' -mtime +"$keep_days" -delete
  sleep "$interval"
done
