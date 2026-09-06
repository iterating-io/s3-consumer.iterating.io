package io.iterating.s3.consumer.messaging;

import java.time.Instant;
import java.util.List;

public record S3ObjectsReconciled(
        String eventId,
        String sourceEventId,
        Instant completedAt,
        long draftId,
        String bucket,
        String path,
        List<String> objects) {
}
