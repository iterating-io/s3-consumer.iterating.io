#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

# Make the script usable both with and without the direnv hook installed.
source .envrc

required_variables=(
    SPRING_PROFILES_ACTIVE
    NATS_SERVERS
    NATS_CONNECTION_NAME
    NATS_CREDENTIALS_PATH
    NATS_STREAM
    NATS_DURABLE
    NATS_FILTER_SUBJECT
    NATS_BATCH_SIZE
    NATS_FETCH_TIMEOUT
    NATS_IDLE_DELAY
    S3_BACKUP_BUCKET
    AWS_REGION
    AWS_ENDPOINT_URL
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
)
for variable in "${required_variables[@]}"; do
    if [[ -z "${!variable:-}" ]]; then
        echo "Missing required environment variable: $variable" >&2
        exit 1
    fi
done

exec ./gradlew :consumer:bootRun "$@"
