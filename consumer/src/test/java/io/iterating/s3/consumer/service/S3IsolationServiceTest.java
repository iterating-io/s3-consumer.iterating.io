package io.iterating.s3.consumer.service;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.InOrder;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.iterating.s3.consumer.config.S3ReconciliationProperties;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

class S3ReconciliationServiceTest {

    private final S3Client s3Client = org.mockito.Mockito.mock(S3Client.class);
    private final S3ReconciliationService service = new S3ReconciliationService(
            s3Client,
            new S3ReconciliationProperties("backup-bucket", "ap-northeast-2", null));

    @Test
    void copiesVerifiesAndDeletesSourceObject() {
        GetObjectResponse getResp = GetObjectResponse.builder().contentLength(12L).build();
        ResponseInputStream<GetObjectResponse> sourceStream = responseStream(getResp);
        ResponseInputStream<GetObjectResponse> backupStream = responseStream(getResp);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(sourceStream)
                .thenReturn(backupStream);
        when(s3Client.copyObject(any(CopyObjectRequest.class))).thenReturn(CopyObjectResponse.builder().build());
        when(s3Client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(DeleteObjectResponse.builder().build());

        S3ReconciliationResult result = service.backup("source-bucket", "path/object.txt");

        assertThat(result.sourceDeleted()).isTrue();
        assertThat(result.backupBucket()).isEqualTo("backup-bucket");
        assertThat(result.backupKey()).startsWith("source-bucket/").endsWith("/object.txt");

        InOrder inOrder = inOrder(s3Client);
        inOrder.verify(s3Client).getObject(any(GetObjectRequest.class));
        inOrder.verify(s3Client).copyObject(any(CopyObjectRequest.class));
        inOrder.verify(s3Client).getObject(any(GetObjectRequest.class));
        inOrder.verify(s3Client).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void restoresMissingSourceFromBackupAndRetainsBackup() {
        NoSuchKeyException notFound = NoSuchKeyException.builder().statusCode(404).message("not found").build();
        GetObjectResponse backupResponse = GetObjectResponse.builder().contentLength(12L).eTag("etag").build();
        GetObjectResponse restoredResponse = GetObjectResponse.builder().contentLength(12L).eTag("etag").build();
        ResponseInputStream<GetObjectResponse> backupStream = responseStream(backupResponse);
        ResponseInputStream<GetObjectResponse> restoredStream = responseStream(restoredResponse);
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(notFound)
                .thenReturn(backupStream)
                .thenReturn(restoredStream);
        when(s3Client.copyObject(any(CopyObjectRequest.class))).thenReturn(CopyObjectResponse.builder().build());

        service.restoreIfMissing("source-bucket", "path/object.txt");

        verify(s3Client, times(3)).getObject(any(GetObjectRequest.class));
        verify(s3Client).copyObject(any(CopyObjectRequest.class));
        verify(s3Client, never()).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void skipsWhenSourceAndBackupAreBothMissing() {
        NoSuchKeyException notFound = NoSuchKeyException.builder().statusCode(404).message("not found").build();
        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(notFound);

        service.restoreIfMissing("source-bucket", "path/object.txt");
        verify(s3Client, times(2)).getObject(any(GetObjectRequest.class));
        verify(s3Client, never()).copyObject(any(CopyObjectRequest.class));
        verify(s3Client, never()).deleteObject(any(DeleteObjectRequest.class));
    }

    @SuppressWarnings("unchecked")
    private static ResponseInputStream<GetObjectResponse> responseStream(GetObjectResponse response) {
        ResponseInputStream<GetObjectResponse> stream = org.mockito.Mockito.mock(ResponseInputStream.class);
        when(stream.response()).thenReturn(response);
        return stream;
    }
}
