# S3 Consumer

Java 25 worker service that consumes NATS JetStream messages and reconciles S3 objects. For each valid message, the service copies matching source objects to a backup location, verifies the backup metadata, deletes the original object, and acknowledges the NATS message only after the workflow succeeds.

## Stack

- Java 25
- Gradle Kotlin DSL
- Spring Boot worker runtime
- NATS Java client
- AWS SDK for Java v2 S3
- JUnit 5, Mockito, Testcontainers

## Message Contract

The consumer expects JSON payloads like this:

```json
{
    "eventId": "1d47b09b-67d1-438c-a86e-8d1f611ac080",
    "sourceEventId": "a4f3c2d1-7b68-4e90-9abc-1234567890ab",
    "requestedAt": "2026-08-23T12:00:00Z",
    "sourceEvent": "blog.draft.updated",
    "draftId": 26,
    "bucket": "source-bucket",
    "path": "public/draft/21/",
    "objects": ["first.png", "second.png"]
}
```

Copy expiry payloads target a share prefix and include no object names:

```json
{
    "eventId": "5a5d5b3e-8e47-4ef0-a6bd-51ef3062efb5",
    "sourceEventId": "a4f3c2d1-7b68-4e90-9abc-1234567890ab",
    "requestedAt": "2026-08-23T12:00:00Z",
    "sourceEvent": "share.copy.expired",
    "shareId": "f5b8ed5e-390d-4df3-88cb-810ac47f20db",
    "bucket": "source-bucket",
    "path": "share/f5b8ed5e-390d-4df3-88cb-810ac47f20db/",
    "objects": []
}
```

- Common required fields are UUID strings `eventId` and `sourceEventId`, an ISO-8601 `requestedAt`, non-blank `bucket`, string `path`, and an `objects` array of non-blank strings. `sourceEvent` is optional and descriptive; the consumer does not restrict its value. Unknown JSON fields are rejected.
- Blog reconciliation requires a positive integer `draftId` and omits `shareId`.
- Copy expiry targets `path` `share/<canonical-lowercase-UUID>/`, omits `draftId`, and supplies an empty `objects` array. `shareId` is optional; when present it must match the UUID in the path.
- After successful reconciliation, the consumer publishes to `s3.objects.reconciled`. Completion has a deterministic `eventId`, uses the input `eventId` as `sourceEventId`, and repeats `bucket`, `path`, and `objects`. Share completions include the path UUID as `shareId` and omit `draftId`; blog completions include `draftId` and omit `shareId`.
- `path` is a common prefix and must end in `/` when non-empty. `objects` contains filenames relative to `path`; only specified objects are backed up and deleted.

## Configuration

Runtime configuration is bound from environment variables through `consumer/src/main/resources/application.yml`.

| Environment variable    | Default                 | Description                                                                                                       |
| ----------------------- | ----------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `NATS_SERVERS`          | `nats://localhost:4222` | Comma-separated NATS server URLs                                                                                  |
| `NATS_CREDENTIALS_PATH` | empty                   | Path to NATS `.creds` file or inline credentials content. Multiline values and escaped `\n` values are supported. |
| `NATS_STREAM`           | `S3_EVENTS`             | JetStream stream name                                                                                             |
| `NATS_DURABLE`          | `s3-isolator`           | Durable consumer name                                                                                             |
| `NATS_FILTER_SUBJECT`   | `s3.objects.reconcile`  | Subject/filter to consume                                                                                         |
| `NATS_BATCH_SIZE`       | `10`                    | Pull batch size                                                                                                   |
| `S3_BACKUP_BUCKET`      | empty                   | Required backup bucket; backups are stored as `<backup-bucket>/<source-bucket>/<original/path>`                   |
| `AWS_REGION`            | `ap-northeast-2`        | AWS region                                                                                                        |
| `AWS_ENDPOINT_URL`      | empty                   | Optional endpoint override for LocalStack or S3-compatible services                                               |
| `AWS_ACCESS_KEY_ID`     | empty                   | Optional static access key used instead of the default AWS credentials chain                                      |
| `AWS_SECRET_ACCESS_KEY` | empty                   | Optional static secret key used instead of the default AWS credentials chain                                      |
| `AWS_SESSION_TOKEN`     | empty                   | Optional session token for temporary credentials                                                                  |

**Security note:** The `consumer/src/main/resources/application.yml` file previously contained example AWS credentials. These have been removed; do not commit real credentials. For local testing, set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally `AWS_SESSION_TOKEN` in your environment.

## Run locally

Copy the local-only environment template, set the NATS credentials path and backup bucket, then run the worker. `.envrc` supplies the non-sensitive AsyncAPI defaults, including the `S3_EVENTS` stream and `s3.objects.reconcile` subject.

```bash
cp .env.example .env
# Edit .env with your local values.
./run.sh
```

If you use direnv, run `direnv allow` once to load `.envrc` automatically.

### Local profile (development)

The local profile is active by default through `.envrc`. In CI or production, set `SPRING_PROFILES_ACTIVE=prod` and supply all required environment variables externally.

## Development

Run tests:

```bash
./gradlew :consumer:test
```

Build the application:

```bash
./gradlew :consumer:build
```

Run directly with explicit settings:

```bash
S3_BACKUP_BUCKET=my-backup-bucket \
NATS_SERVERS=nats://localhost:4222 \
NATS_CREDENTIALS_PATH="/path/to/user.creds" \
./gradlew :consumer:bootRun
```

## NATS Behavior

The worker uses a durable JetStream pull consumer. Successful messages are acknowledged after S3 copy, backup verification, and source deletion finish. Invalid payloads are terminated. Retryable processing errors are negatively acknowledged so JetStream can redeliver them according to the consumer policy.

For S3-compatible backends that do not reliably support `HeadObject`, the consumer falls back to a ranged `GetObject` request (`bytes=0-0`) to obtain ETag and content-length metadata for verification.

## Required S3 Permissions

The runtime identity needs these permissions for the relevant buckets and prefixes:

- `s3:GetObject`
- `s3:PutObject` or copy permission to the backup bucket
- `s3:DeleteObject`

`s3:HeadObject` is recommended when the backend supports it, but the consumer can fall back to `GetObject` metadata reads for S3-compatible backends that reject `HeadObject`.

For versioned buckets, include permissions that cover object versions.

## Recent Changes

- **Date:** 2026-06-28
- **Summary:** Collected recent commits and current working-tree modifications for documentation.

### Recent commits

- `07c3230` (2026-06-21) — test(nats): improve consumer testability and add local test helpers
- `7769d15` (2026-06-16) — chore(session): remove temporary session file after review
- `bd6b2ce` (2026-06-16) — chore(session): add session-20260616-204532.md (temporary)
- `5cd657e` (2026-06-16) — feat(nats,consumer): extract NATS into :nats module; rename S3 isolation -> reconciliation
- `f6e0903` (2026-06-16) — stage
- `70e64ad` (2026-05-30) — Initial commit

### Working-tree (uncommitted) changes

- Modified: `README.md` (this document)
- Modified: `consumer/build.gradle.kts` (dependency / build tweaks)
- Modified: `consumer/src/main/java/io/iterating/s3/consumer/config/ConsumerConfiguration.java` (imports/config wiring)
- Modified: `consumer/src/main/java/io/iterating/s3/consumer/config/S3Configuration.java`
- Modified: `consumer/src/main/java/io/iterating/s3/consumer/config/S3ReconciliationProperties.java` (new/updated properties binding)
- Modified: `consumer/src/main/java/io/iterating/s3/consumer/config/S3Configuration.java`
- Modified: `consumer/src/main/java/io/iterating/s3/consumer/messaging/S3ObjectEvent.java` (validation/keys helper)
- Modified: `consumer/src/main/java/io/iterating/s3/consumer/messaging/S3ObjectEventMessageHandler.java` (event handling/exclusion logic)
- Modified: `consumer/src/main/java/io/iterating/s3/consumer/service/S3ReconciliationService.java` (backup, verify, delete flow; GET fallback metadata; improved error guidance)
- Modified: `consumer/src/main/resources/application.yml` (configuration defaults and local AWS credentials for testing)
- Modified: `consumer/src/test/java/...` (tests updated: `S3ObjectEventTest`, `S3IsolationServiceTest`)
- Modified: `nats-check.sh`, `nats/build.gradle.kts`, and `nats/src/...` (NATS module updates and tests)
- Deleted: `local-run.sh`, `local-test.sh`
- Untracked: `.github/sessions/session-20260625-120000.md`, `.github/sessions/session-20260628-000000.md` (session files)
- Untracked (new): `consumer/src/main/java/io/iterating/s3/consumer/config/AwsCredentialsConfiguration.java`, `consumer/src/main/java/io/iterating/s3/consumer/config/AwsProperties.java`

### Notes / Next steps

- Review uncommitted changes and sensitive values before committing (the `application.yml` contains local AWS credential placeholders).
- Decide whether to commit new `AwsCredentialsConfiguration`/`AwsProperties` files and the session files under `.github/sessions`.
- After review, commit the working-tree changes with a concise commit message (e.g. `docs(readme): add recent changes summary`).

If you want, I can (a) expand any file-specific summaries with excerpts, (b) create a CHANGELOG.md with structured entries, or (c) produce a suggested commit message and stage the changes.
