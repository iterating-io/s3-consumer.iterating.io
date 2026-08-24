#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

# Make the script usable both with and without the direnv hook installed.
source .envrc

required_variables=(NATS_CREDENTIALS_PATH S3_BACKUP_BUCKET)
for variable in "${required_variables[@]}"; do
    if [[ -z "${!variable:-}" ]]; then
        echo "Missing $variable. Copy .env.example to .env and set it before running." >&2
        exit 1
    fi
done

exec ./gradlew :consumer:bootRun "$@"
