package io.iterating.s3.consumer.messaging;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.junit.jupiter.api.Test;

class S3ObjectEventTest {

    private static final String EVENT_ID = "1d47b09b-67d1-438c-a86e-8d1f611ac080";
    private static final String SOURCE_EVENT_ID = "a4f3c2d1-7b68-4e90-9abc-1234567890ab";
    private static final Instant REQUESTED_AT = Instant.parse("2026-08-23T12:00:00Z");

    private S3ObjectEvent event(String bucket, String path, List<String> objects) {
        return new S3ObjectEvent(
                EVENT_ID, SOURCE_EVENT_ID, REQUESTED_AT, "blog.draft.updated", 26, bucket, path, objects);
    }

    @Test
    void validatesRequiredFields() {
        S3ObjectEvent event = event("source-bucket", "public/draft/21/", List.of("file.png"));

        assertThatCode(event::validate).doesNotThrowAnyException();
    }

    @Test
    void rejectsMissingBucket() {
        S3ObjectEvent event = event("", "public/draft/21/", List.of("file.png"));

        assertThatThrownBy(event::validate)
                .isInstanceOf(InvalidS3ObjectEventException.class)
                .hasMessageContaining("bucket");
    }

    @Test
    void rejectsMissingPath() {
        S3ObjectEvent event = event("source-bucket", null, List.of("file.png"));

        assertThatThrownBy(event::validate)
                .isInstanceOf(InvalidS3ObjectEventException.class)
                .hasMessageContaining("path");
    }

    @Test
    void rejectsMissingObjects() {
        S3ObjectEvent event = event("source-bucket", "public/draft/21/", null);

        assertThatThrownBy(event::validate)
                .isInstanceOf(InvalidS3ObjectEventException.class)
                .hasMessageContaining("objects");
    }

    @Test
    void keysJoinsPathAndObjects() {
        S3ObjectEvent event = event("source-bucket", "public/draft/21/", List.of("a.png", "b.png"));

        assertThat(event.keys()).containsExactly("public/draft/21/a.png", "public/draft/21/b.png");
    }

    @Test
    void rejectsPathWithoutTrailingSlash() {
        S3ObjectEvent event = event("source-bucket", "public/draft/21", List.of("a.png"));

        assertThatThrownBy(event::validate)
                .isInstanceOf(InvalidS3ObjectEventException.class)
                .hasMessageContaining("slash");
    }
}
