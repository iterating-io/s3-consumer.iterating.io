package io.iterating.s3.consumer.messaging;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.time.Instant;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import io.iterating.s3.consumer.service.S3ReconciliationException;
import io.iterating.s3.consumer.service.S3ReconciliationService;
import io.iterating.s3.nats.messaging.InvalidMessageException;
import io.iterating.s3.nats.messaging.MessageHandler;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3ObjectEventMessageHandler implements MessageHandler {

    private static final Logger log = LoggerFactory.getLogger(S3ObjectEventMessageHandler.class);
    private static final Set<String> PAYLOAD_FIELDS = Set.of(
            "eventId", "sourceEventId", "requestedAt", "sourceEvent", "draftId", "bucket", "path", "objects");
    private static final Set<String> SOURCE_EVENTS = Set.of(
            "blog.draft.created", "blog.draft.updated", "blog.draft.deleted");

    private final ObjectMapper objectMapper;
    private final S3ReconciliationService reconciliationService;
    private final S3ObjectsReconciledPublisher reconciledPublisher;

    public S3ObjectEventMessageHandler(
            ObjectMapper objectMapper,
            S3ReconciliationService reconciliationService,
            S3ObjectsReconciledPublisher reconciledPublisher) {
        this.objectMapper = objectMapper;
        this.reconciliationService = reconciliationService;
        this.reconciledPublisher = reconciledPublisher;
    }

    @Override
    public void handle(byte[] payload, String subject) throws Exception {
        try {
            S3ObjectEvent event = parsePayload(payload);
            List<String> keys = event.keys();
            Set<String> keepKeys = new HashSet<>(keys);

            log.info("S3 objects reconcile requested eventId={} subject={} sourceBucket={} path={} objectCount={}",
                    event.eventId(), subject, event.bucket(), event.path(), keys.size());
            log.info("S3 prefix reconciliation starting eventId={} source=s3://{}/{} keepObjectCount={}",
                    event.eventId(), event.bucket(), event.path(), keepKeys.size());
            try {
                reconciliationService.reconcile(event.bucket(), event.path(), keepKeys);
            } catch (Exception exception) {
                log.error(
                        "S3 prefix reconciliation failed eventId={} subject={} stage=BACKUP_UNREFERENCED source=s3://{}/{} exceptionType={} message={}",
                        event.eventId(), subject, event.bucket(), event.path(),
                        exception.getClass().getSimpleName(), exception.getMessage());
                throw exception;
            }
            log.info("S3 prefix reconciliation finished eventId={} source=s3://{}/{}",
                    event.eventId(), event.bucket(), event.path());

            List<Exception> restoreFailures = new ArrayList<>();
            for (String key : keys) {
                log.info("S3 referenced object check starting eventId={} source=s3://{}/{}",
                        event.eventId(), event.bucket(), key);
                try {
                    reconciliationService.restoreIfMissing(event.bucket(), key);
                } catch (Exception exception) {
                    restoreFailures.add(exception);
                    log.error(
                            "S3 referenced object check failed; continuing eventId={} subject={} stage=KEEP_OR_RESTORE source=s3://{}/{} exceptionType={} message={}",
                            event.eventId(),
                            subject,
                            event.bucket(),
                            key,
                            exception.getClass().getSimpleName(),
                            exception.getMessage());
                    continue;
                }
                log.info("S3 referenced object check finished eventId={} source=s3://{}/{}",
                        event.eventId(), event.bucket(), key);
            }
            if (!restoreFailures.isEmpty()) {
                io.iterating.s3.consumer.service.S3ReconciliationException aggregate =
                        new io.iterating.s3.consumer.service.S3ReconciliationException(
                                "Failed to keep or restore " + restoreFailures.size()
                                        + " referenced object(s) for eventId=" + event.eventId(),
                                restoreFailures.getFirst());
                restoreFailures.stream().skip(1).forEach(aggregate::addSuppressed);
                throw aggregate;
            }
            log.info("Publishing S3ObjectsReconciled sourceEventId={} subject={} objectCount={}",
                    event.eventId(), NatsS3ObjectsReconciledPublisher.SUBJECT, event.objects().size());
            try {
                reconciledPublisher.publish(new S3ObjectsReconciled(
                        uuidV5(event.eventId(), NatsS3ObjectsReconciledPublisher.SUBJECT),
                        event.eventId(), Instant.now(), event.draftId(), event.bucket(), event.path(), event.objects()));
            } catch (Exception exception) {
                log.error(
                        "S3 reconciliation completion publish failed sourceEventId={} stage=PUBLISH_COMPLETION subject={} exceptionType={} message={}",
                        event.eventId(),
                        NatsS3ObjectsReconciledPublisher.SUBJECT,
                        exception.getClass().getSimpleName(),
                        exception.getMessage());
                throw exception;
            }
            log.info("S3 objects reconciled sourceEventId={} subject={} sourceBucket={} path={} objectCount={}",
                    event.eventId(), subject, event.bucket(), event.path(), keys.size());
        } catch (JsonProcessingException | InvalidS3ObjectEventException exception) {
            throw new InvalidMessageException("Invalid S3 object event", exception);
        } catch (java.io.IOException exception) {
            throw new InvalidMessageException("Failed to parse S3 object event", exception);
        } catch (S3ReconciliationException exception) {
            // If the reconciliation failed due to an S3 403 (access denied), treat it as a permanent
            // processing error and terminate the message instead of scheduling retries.
            Throwable cause = exception.getCause();
            while (cause != null) {
                if (cause instanceof S3Exception) {
                    S3Exception s3ex = (S3Exception) cause;
                    if (s3ex.statusCode() == 403) {
                        throw new InvalidMessageException("Access denied while reconciling S3 object", exception);
                    }
                }
                cause = cause.getCause();
            }
            // Otherwise propagate runtime exception to trigger retry behavior
            throw exception;
        }
    }

    private S3ObjectEvent parsePayload(byte[] payload) throws java.io.IOException, InvalidS3ObjectEventException {
        JsonNode root = objectMapper.readTree(payload);
        if (root == null || !root.isObject()) {
            throw new InvalidS3ObjectEventException("S3 object event must be a JSON object");
        }

        Iterator<String> fields = root.fieldNames();
        while (fields.hasNext()) {
            String field = fields.next();
            if (!PAYLOAD_FIELDS.contains(field)) {
                throw new InvalidS3ObjectEventException("S3 object event contains an unsupported field: " + field);
            }
        }

        JsonNode eventId = root.get("eventId");
        JsonNode sourceEventId = root.get("sourceEventId");
        JsonNode requestedAt = root.get("requestedAt");
        JsonNode sourceEvent = root.get("sourceEvent");
        JsonNode draftId = root.get("draftId");
        JsonNode bucket = root.get("bucket");
        JsonNode path = root.get("path");
        JsonNode objects = root.get("objects");
        if (eventId == null || !eventId.isTextual() || !isUuid(eventId.textValue())) {
            throw new InvalidS3ObjectEventException("S3 object event eventId must be a UUID string");
        }
        if (sourceEventId == null || !sourceEventId.isTextual() || !isUuid(sourceEventId.textValue())) {
            throw new InvalidS3ObjectEventException("S3 object event sourceEventId must be a UUID string");
        }
        if (requestedAt == null || !requestedAt.isTextual() || !isInstant(requestedAt.textValue())) {
            throw new InvalidS3ObjectEventException("S3 object event requestedAt must be a date-time string");
        }
        if (sourceEvent == null || !sourceEvent.isTextual() || !SOURCE_EVENTS.contains(sourceEvent.textValue())) {
            throw new InvalidS3ObjectEventException("S3 object event sourceEvent is invalid");
        }
        if (draftId == null || !draftId.isIntegralNumber() || draftId.longValue() < 1) {
            throw new InvalidS3ObjectEventException("S3 object event draftId must be a positive integer");
        }
        if (bucket == null || !bucket.isTextual() || bucket.textValue().isBlank()) {
            throw new InvalidS3ObjectEventException("S3 object event bucket must be a non-blank string");
        }
        if (path == null || !path.isTextual()) {
            throw new InvalidS3ObjectEventException("S3 object event path must be a string");
        }
        if (objects == null || !objects.isArray()) {
            throw new InvalidS3ObjectEventException("S3 object event objects must be an array");
        }
        for (JsonNode object : objects) {
            if (!object.isTextual() || object.textValue().isBlank()) {
                throw new InvalidS3ObjectEventException("S3 object event objects must contain non-blank strings");
            }
        }

        S3ObjectEvent event = objectMapper.treeToValue(root, S3ObjectEvent.class);
        event.validate();
        return event;
    }

    private static String uuidV5(String namespace, String name) {
        try {
            UUID namespaceUuid = UUID.fromString(namespace);
            ByteBuffer namespaceBytes = ByteBuffer.allocate(16)
                    .putLong(namespaceUuid.getMostSignificantBits())
                    .putLong(namespaceUuid.getLeastSignificantBits());
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            sha1.update(namespaceBytes.array());
            byte[] hash = sha1.digest(name.getBytes(StandardCharsets.UTF_8));
            hash[6] = (byte) ((hash[6] & 0x0f) | 0x50);
            hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
            ByteBuffer uuidBytes = ByteBuffer.wrap(hash);
            return new UUID(uuidBytes.getLong(), uuidBytes.getLong()).toString();
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to generate completion event ID", exception);
        }
    }

    private static boolean isUuid(String value) {
        try {
            java.util.UUID.fromString(value);
            return true;
        } catch (IllegalArgumentException exception) {
            return false;
        }
    }

    private static boolean isInstant(String value) {
        try {
            Instant.parse(value);
            return true;
        } catch (java.time.format.DateTimeParseException exception) {
            return false;
        }
    }
}
