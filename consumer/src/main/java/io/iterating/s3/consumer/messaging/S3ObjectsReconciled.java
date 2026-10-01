package io.iterating.s3.consumer.messaging;

import java.time.Instant;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record S3ObjectsReconciled(
        String eventId,
        String sourceEventId,
        Instant completedAt,
        Long draftId,
        String shareId,
        String bucket,
        String path,
        List<String> objects) {
    public S3ObjectsReconciled(String eventId, String sourceEventId, Instant completedAt,
            long draftId, String bucket, String path, List<String> objects) {
        this(eventId, sourceEventId, completedAt, draftId, null, bucket, path, objects);
    }
}
