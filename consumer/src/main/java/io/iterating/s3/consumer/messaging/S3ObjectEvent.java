package io.iterating.s3.consumer.messaging;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record S3ObjectEvent(
        String eventId,
        String sourceEventId,
        Instant requestedAt,
        String sourceEvent,
        Long draftId,
        String shareId,
        String bucket,
        String path,
        List<String> objects) {

    public S3ObjectEvent(String eventId, String sourceEventId, Instant requestedAt, String sourceEvent,
            long draftId, String bucket, String path, List<String> objects) {
        this(eventId, sourceEventId, requestedAt, sourceEvent, draftId, null, bucket, path, objects);
    }

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
        if (isSharePath(path)) {
            String pathShareId = shareIdFromPath(path);
            if (draftId != null || (shareId != null && !shareId.equals(pathShareId))
                    || objects == null || !objects.isEmpty()) {
                throw new InvalidS3ObjectEventException("copy expiry must target its own share prefix with no kept objects");
            }
        } else if (draftId == null || draftId < 1 || shareId != null) {
            throw new InvalidS3ObjectEventException("blog event requires draftId and no shareId");
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

    private static String shareIdFromPath(String path) {
        if (path == null || !path.startsWith("share/") || !path.endsWith("/")) {
            throw new InvalidS3ObjectEventException("copy expiry path must be share/<UUID>/");
        }
        String value = path.substring("share/".length(), path.length() - 1);
        try {
            String canonicalValue = UUID.fromString(value).toString();
            if (!canonicalValue.equals(value)) {
                throw new IllegalArgumentException("non-canonical UUID");
            }
            return canonicalValue;
        } catch (IllegalArgumentException exception) {
            throw new InvalidS3ObjectEventException("copy expiry path must be share/<UUID>/");
        }
    }

    private static boolean isSharePath(String path) {
        return path != null && path.startsWith("share/");
    }

    public String effectiveShareId() {
        return isSharePath(path) ? shareIdFromPath(path) : shareId;
    }

    public List<String> keys() {
        return objects.stream()
                .map(object -> path + object)
                .toList();
    }
}
