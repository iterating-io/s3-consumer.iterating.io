package io.iterating.s3.consumer.messaging;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.iterating.s3.consumer.service.S3ReconciliationService;
import io.iterating.s3.nats.messaging.InvalidMessageException;

class S3ObjectEventMessageHandlerTest {

    private final S3ReconciliationService reconciliationService = mock(S3ReconciliationService.class);
    private final S3ObjectsReconciledPublisher reconciledPublisher = mock(S3ObjectsReconciledPublisher.class);
    private final S3ObjectEventMessageHandler handler = new S3ObjectEventMessageHandler(
            new ObjectMapper().registerModule(new JavaTimeModule()), reconciliationService, reconciledPublisher);

    @Test
    void reconcilesOnlyObjectsNamedInTheMessage() throws Exception {
        handler.handle("""
                {"commandId":"1d47b09b-67d1-438c-a86e-8d1f611ac080","requestedAt":"2026-08-23T12:00:00Z","sourceEvent":"blog.draft.updated","draftId":26,"bucket":"dev.blog.iterating.io","path":"public/draft/26/","objects":["first.png","second.png"]}
                """.getBytes(), "s3.objects.reconcile");

        verify(reconciliationService).reconcile(
                "dev.blog.iterating.io",
                "public/draft/26/",
                java.util.Set.of("public/draft/26/first.png", "public/draft/26/second.png"));
        verify(reconciliationService).restoreIfMissing(
                "dev.blog.iterating.io", "public/draft/26/first.png");
        verify(reconciliationService).restoreIfMissing(
                "dev.blog.iterating.io", "public/draft/26/second.png");
        verifyNoMoreInteractions(reconciliationService);
        ArgumentCaptor<S3ObjectsReconciled> result = ArgumentCaptor.forClass(S3ObjectsReconciled.class);
        verify(reconciledPublisher).publish(result.capture());
        org.assertj.core.api.Assertions.assertThat(result.getValue())
                .usingRecursiveComparison()
                .ignoringFields("completedAt")
                .isEqualTo(new S3ObjectsReconciled(
                        "1d47b09b-67d1-438c-a86e-8d1f611ac080", Instant.EPOCH, 26,
                        "dev.blog.iterating.io", "public/draft/26/", List.of("first.png", "second.png")));
    }

    @Test
    void publishesCompletionWhenSourceObjectWasAlreadyMissing() throws Exception {
        handler.handle("""
                {"commandId":"0c7eded4-079b-491e-99b9-28e3f25f0469","requestedAt":"2026-08-29T23:42:38.613643650Z","sourceEvent":"blog.draft.updated","draftId":30,"bucket":"dev.blog.iterating.io","path":"public/draft/30/","objects":["missing.png"]}
                """.getBytes(), "s3.objects.reconcile");

        ArgumentCaptor<S3ObjectsReconciled> completion = ArgumentCaptor.forClass(S3ObjectsReconciled.class);
        verify(reconciledPublisher).publish(completion.capture());
        org.assertj.core.api.Assertions.assertThat(completion.getValue().commandId())
                .isEqualTo("0c7eded4-079b-491e-99b9-28e3f25f0469");
        org.assertj.core.api.Assertions.assertThat(completion.getValue().objects())
                .containsExactly("missing.png");
    }

    @Test
    void reconcilesEntirePrefixWhenKeepListIsEmpty() throws Exception {
        handler.handle("""
                {"commandId":"49aff39b-9486-4486-b5b1-5bd4d646b988","requestedAt":"2026-08-31T12:41:33.121765898Z","sourceEvent":"blog.draft.created","draftId":31,"bucket":"dev.blog.iterating.io","path":"public/draft/31/","objects":[]}
                """.getBytes(), "s3.objects.reconcile");

        verify(reconciliationService).reconcile(
                "dev.blog.iterating.io", "public/draft/31/", java.util.Set.of());
    }

    @Test
    void rejectsPayloadFieldsOutsideTheAsyncApiSchema() {
        assertThatThrownBy(() -> handler.handle("""
                {"commandId":"1d47b09b-67d1-438c-a86e-8d1f611ac080","requestedAt":"2026-08-23T12:00:00Z","sourceEvent":"blog.draft.updated","draftId":26,"bucket":"dev.blog.iterating.io","path":"public/draft/26/","objects":[],"extra":true}
                """.getBytes(), "s3.objects.reconcile"))
                .isInstanceOf(InvalidMessageException.class)
                .hasMessageContaining("Invalid S3 object event");
    }

    @Test
    void rejectsMissingRequiredFields() {
        assertThatThrownBy(() -> handler.handle("""
                {"commandId":"1d47b09b-67d1-438c-a86e-8d1f611ac080","requestedAt":"2026-08-23T12:00:00Z","sourceEvent":"blog.draft.updated","draftId":26,"bucket":"dev.blog.iterating.io","path":"public/draft/26/"}
                """.getBytes(), "s3.objects.reconcile"))
                .isInstanceOf(InvalidMessageException.class)
                .hasMessageContaining("Invalid S3 object event");
    }
}
