#!/usr/bin/env sh
# Resolve and pin dependencies for the container's Python version (3.12, linux).
set -eu
docker run --rm -v "$(pwd)":/src -w /src python:3.12-slim sh -c '
  python -m venv /tmp/rt && /tmp/rt/bin/pip install -q -r requirements.txt && /tmp/rt/bin/pip freeze --all --exclude pip > requirements.lock &&
  python -m venv /tmp/dev && /tmp/dev/bin/pip install -q -r requirements-dev.txt && /tmp/dev/bin/pip freeze --all --exclude pip > requirements-dev.lock'
echo "requirements.lock and requirements-dev.lock updated"
