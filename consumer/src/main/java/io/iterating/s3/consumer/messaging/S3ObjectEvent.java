package io.iterating.s3.consumer.messaging;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public record S3ObjectEvent(
        String eventId,
        String sourceEventId,
        Instant requestedAt,
        String sourceEvent,
        long draftId,
        String bucket,
        String path,
        List<String> objects) {

    private static final Set<String> SOURCE_EVENTS = Set.of(
            "blog.draft.created", "blog.draft.updated", "blog.draft.deleted");

    public void validate() {
        if (eventId == null) {
            throw new InvalidS3ObjectEventException("S3 object event eventId is required");
        }
        try {
            UUID.fromString(eventId);
        } catch (IllegalArgumentException exception) {
            throw new InvalidS3ObjectEventException("S3 object event eventId must be a UUID");
        }
        if (sourceEventId == null) {
            throw new InvalidS3ObjectEventException("S3 object event sourceEventId is required");
        }
        try {
            UUID.fromString(sourceEventId);
        } catch (IllegalArgumentException exception) {
            throw new InvalidS3ObjectEventException("S3 object event sourceEventId must be a UUID");
        }
        if (requestedAt == null) {
            throw new InvalidS3ObjectEventException("S3 object event requestedAt is required");
        }
        if (!SOURCE_EVENTS.contains(sourceEvent)) {
            throw new InvalidS3ObjectEventException("S3 object event sourceEvent is invalid");
        }
        if (draftId < 1) {
            throw new InvalidS3ObjectEventException("S3 object event draftId must be at least 1");
        }
        if (bucket == null || bucket.isBlank()) {
            throw new InvalidS3ObjectEventException("S3 object event bucket is required");
        }
        if (path == null) {
            throw new InvalidS3ObjectEventException("S3 object event path is required");
        }
        if (!path.isEmpty() && !path.endsWith("/")) {
            throw new InvalidS3ObjectEventException("S3 object event path must end with a slash when non-empty");
        }
        if (objects == null) {
            throw new InvalidS3ObjectEventException("S3 object event objects is required");
        }
        if (objects.stream().anyMatch(object -> object == null || object.isBlank())) {
            throw new InvalidS3ObjectEventException("S3 object event objects must not contain blank entries");
        }
    }

    public List<String> keys() {
        return objects.stream()
                .map(object -> path + object)
                .toList();
    }
}
