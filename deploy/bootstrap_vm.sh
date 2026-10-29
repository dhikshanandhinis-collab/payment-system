#!/usr/bin/env bash
# One-time manual setup of a fresh Ubuntu 22.04/24.04 VM (infra/provision.ps1 automates this on Azure).
# Usage: bash bootstrap_vm.sh <git-clone-url>
set -euo pipefail
repo_url="${1:?usage: bootstrap_vm.sh <git-clone-url>}"
app_dir="$HOME/payment-system"

sudo apt-get update
sudo apt-get install -y docker.io docker-compose-v2 git
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"

[ -d "$app_dir/.git" ] || git clone "$repo_url" "$app_dir"
cd "$app_dir"
if [ ! -f .env ]; then
  cp .env.example .env
  chmod 600 .env
  echo "Created $app_dir/.env - replace every password and set the SMTP_* / ALERT_EMAIL_TO values."
fi

echo "Next: log out and back in (docker group), edit .env, then run: bash deploy/deploy.sh"
