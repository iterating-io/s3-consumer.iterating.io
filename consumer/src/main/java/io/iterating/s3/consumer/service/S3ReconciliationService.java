package io.iterating.s3.consumer.service;

import java.util.Locale;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.iterating.s3.consumer.config.S3ReconciliationProperties;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(S3ReconciliationService.class);
    private static final long VERIFY_BACKUP_DELAY_MILLIS = 3_000L;

    private final S3Client s3Client;
    private final S3ReconciliationProperties properties;

    public S3ReconciliationService(S3Client s3Client, S3ReconciliationProperties properties) {
        this.s3Client = s3Client;
        this.properties = properties;
    }

    public S3ReconciliationResult backup(String sourceBucket, String sourceKey) {
        log.info("Reconciling S3 object: {}/{} -> backupBucket={}", sourceBucket, sourceKey, properties.backupBucket());

        String backupKey = backupKey(sourceBucket, sourceKey);
        log.info("S3 reconciliation plan source=s3://{}/{} destination=s3://{}/{}",
                sourceBucket, sourceKey, properties.backupBucket(), backupKey);
        HeadObjectResponse sourceHead = headSource(sourceBucket, sourceKey, backupKey);
        if (sourceHead == null) {
            return new S3ReconciliationResult(sourceBucket, sourceKey, properties.backupBucket(), backupKey, false);
        }
        copyToBackup(sourceBucket, sourceKey, backupKey);
        verifyBackup(sourceBucket, sourceKey, backupKey, sourceHead.eTag(), sourceHead.contentLength());
        deleteSource(sourceBucket, sourceKey);

        return new S3ReconciliationResult(sourceBucket, sourceKey, properties.backupBucket(), backupKey, true);
    }

    public void restoreIfMissing(String sourceBucket, String sourceKey) {
        String backupKey = backupKey(sourceBucket, sourceKey);
        HeadObjectResponse sourceHead = headSource(sourceBucket, sourceKey, backupKey);
        if (sourceHead != null) {
            log.info("Keeping referenced source object s3://{}/{}", sourceBucket, sourceKey);
            return;
        }
        restoreFromBackup(sourceBucket, sourceKey, backupKey);
    }

    /**
     * Reconcile all objects under a given prefix, excluding any keys present in
     * the provided {@code excludeKeys} set. If {@code prefix} is null/empty,
     * operate on the whole bucket. This method handles pagination and will
     * iterate over all keys under the prefix.
     */
    public void reconcile(String bucket, String prefix, Set<String> excludeKeys) {
        String normalizedPrefix = (prefix == null || prefix.isBlank()) ? "" : (prefix.endsWith("/") ? prefix : prefix + "/");
        String continuationToken = null;
        int processed = 0;
        List<Exception> failures = new ArrayList<>();

        while (true) {
            var builder = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(normalizedPrefix)
                    .maxKeys(1000);
            if (continuationToken != null && !continuationToken.isBlank()) {
                builder.continuationToken(continuationToken);
            }
            ListObjectsV2Request req = builder.build();
            ListObjectsV2Response resp = s3Client.listObjectsV2(req);

            for (var obj : resp.contents()) {
                String key = obj.key();
                if (excludeKeys != null && excludeKeys.contains(key)) {
                    log.info("Skipping excluded key {}/{}", bucket, key);
                    continue;
                }
                try {
                    backup(bucket, key);
                    processed++;
                } catch (Exception exception) {
                    failures.add(exception);
                    log.error("Continuing prefix reconciliation after object failure source=s3://{}/{} exceptionType={} message={}",
                            bucket, key, exception.getClass().getSimpleName(), exception.getMessage());
                }
            }

            String next = resp.nextContinuationToken();
            if (next == null || next.isBlank()) {
                break;
            }
            continuationToken = next;
        }

        log.info("Reconciled {} objects under {}/{} (excluded {} keys)", processed, bucket, normalizedPrefix,
                (excludeKeys == null) ? 0 : excludeKeys.size());
        if (!failures.isEmpty()) {
            S3ReconciliationException aggregate = new S3ReconciliationException(
                    "Failed to reconcile " + failures.size() + " object(s) under s3://" + bucket + "/" + normalizedPrefix,
                    failures.getFirst());
            failures.stream().skip(1).forEach(aggregate::addSuppressed);
            throw aggregate;
        }
    }

    private HeadObjectResponse headSource(String bucket, String key, String backupKey) {
        try {
            return readObjectMetadata(bucket, key, "source");
        } catch (S3Exception exception) {
            int status = exception.statusCode();
            try {
                log.error("Failed to read source object metadata for {}/{} (backupKey={}), status={}, requestId={}, message={}",
                        bucket, key, backupKey, status, exception.requestId(), exception.getMessage());
            } catch (Exception logEx) {
                log.error("Failed to read source object metadata and additionally failed to log details: {}", logEx.getMessage());
            }

            if (status == 404) {
                log.info("Source object is missing s3://{}/{}; attempting restore from backup",
                        bucket, key);
                return null;
            }

            if (status == 403) {
                String guidance = "Access denied (403) reading source object " + bucket + "/" + key
                        + ". Possible causes: incorrect AWS credentials, missing IAM permissions (s3:GetObject, s3:HeadObject),"
                        + " bucket policy or ACL denying access, or cross-account restrictions. Suggested checks: run 'aws sts get-caller-identity'"
                        + " and 'aws s3api head-object' with the same credentials; verify IAM policies include s3:GetObject/s3:HeadObject/s3:PutObject/s3:DeleteObject"
                        + " for the source and backup buckets, and rotate compromised keys. RequestId=" + exception.requestId();
                throw new S3ReconciliationException(guidance, exception);
            }

            throw new S3ReconciliationException("Failed to read source object metadata for " + bucket + "/" + key
                    + " (backupKey=" + backupKey + ", status=" + status + ", requestId=" + exception.requestId() + ") : "
                    + exception.getMessage(), exception);
        }
    }

    private void copyToBackup(String bucket, String key, String backupKey) {
        String backupBucket = properties.backupBucket();
        copyObject(bucket, key, backupBucket, backupKey, "backup");
    }

    private void restoreFromBackup(String sourceBucket, String sourceKey, String backupKey) {
        String backupBucket = properties.backupBucket();
        HeadObjectResponse backupHead;
        try {
            backupHead = readObjectMetadata(backupBucket, backupKey,
                    "restore backup for missing source " + sourceBucket + "/" + sourceKey);
        } catch (S3Exception exception) {
            if (exception.statusCode() == 404) {
                log.warn("Cannot restore missing source s3://{}/{} because backup s3://{}/{} is also missing; skipping",
                        sourceBucket, sourceKey, backupBucket, backupKey);
                return;
            }
            throw exception;
        }

        copyObject(backupBucket, backupKey, sourceBucket, sourceKey, "restore");
        verifyRestoredSource(sourceBucket, sourceKey, backupBucket, backupKey,
                backupHead.eTag(), backupHead.contentLength());
        log.info("S3 source restore succeeded source=s3://{}/{} backupRetained=s3://{}/{}",
                sourceBucket, sourceKey, backupBucket, backupKey);
    }

    private void copyObject(
            String fromBucket,
            String fromKey,
            String toBucket,
            String toKey,
            String purpose) {
        log.info("Calling S3 CopyObject purpose={} source=s3://{}/{} destination=s3://{}/{}",
                purpose, fromBucket, fromKey, toBucket, toKey);
        try {
            var response = s3Client.copyObject(CopyObjectRequest.builder()
                    .sourceBucket(fromBucket)
                    .sourceKey(fromKey)
                    .bucket(toBucket)
                    .key(toKey)
                    .build());
            log.info("S3 CopyObject succeeded purpose={} source=s3://{}/{} destination=s3://{}/{} eTag={} requestId={}",
                    purpose, fromBucket, fromKey, toBucket, toKey,
                    response == null || response.copyObjectResult() == null ? null : response.copyObjectResult().eTag(),
                    response == null || response.responseMetadata() == null
                            ? null
                            : response.responseMetadata().requestId());
        } catch (S3Exception exception) {
            logS3Failure("CopyObject (purpose=" + purpose + ")",
                    fromBucket, fromKey, toBucket, toKey, exception);
            throw exception;
        }
    }

    private void verifyRestoredSource(
            String sourceBucket,
            String sourceKey,
            String backupBucket,
            String backupKey,
            String expectedETag,
            Long expectedContentLength) {
        HeadObjectResponse restoredHead = readObjectMetadata(sourceBucket, sourceKey,
                "verify restored source from " + backupBucket + "/" + backupKey);
        verifyMetadata("Restored source", sourceBucket, sourceKey,
                expectedETag, expectedContentLength, restoredHead);
    }

    private void verifyBackup(String bucket, String key, String backupKey, String expectedETag, Long expectedContentLength) {
        try {
            log.info("Waiting {}ms before verifying backup object {}/{}", VERIFY_BACKUP_DELAY_MILLIS,
                    properties.backupBucket(), backupKey);
            Thread.sleep(VERIFY_BACKUP_DELAY_MILLIS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new S3ReconciliationException("Interrupted while waiting to verify backup object "
                    + properties.backupBucket() + "/" + backupKey, exception);
        }

        HeadObjectResponse backupHead = readObjectMetadata(properties.backupBucket(), backupKey,
                "verify backup for source " + bucket + "/" + key);

        verifyMetadata("Backup", bucket, key, expectedETag, expectedContentLength, backupHead);
    }

    private void verifyMetadata(
            String label,
            String bucket,
            String key,
            String expectedETag,
            Long expectedContentLength,
            HeadObjectResponse actualHead) {
        String actualETag = actualHead.eTag();
        if (expectedETag != null && actualETag != null) {
            String normExpected = normalizeETag(expectedETag);
            String normActual = normalizeETag(actualETag);
            if (!normExpected.equals(normActual)) {
                throw new S3ReconciliationException(label + " ETag mismatch for " + bucket + "/" + key
                        + " (expected=" + normExpected + ", actual=" + normActual + ")");
            }
            return;
        }

        log.warn("Unable to verify {} by ETag for {}/{}; falling back to size comparison", label, bucket, key);
        if (expectedContentLength != null && actualHead.contentLength() != expectedContentLength) {
            throw new S3ReconciliationException(label + " size mismatch for " + bucket + "/" + key);
        }
    }

    private static String normalizeETag(String eTag) {
        if (eTag == null) {
            return null;
        }
        return eTag.replace("\"", "").trim().toLowerCase(Locale.ROOT);
    }

    private void deleteSource(String bucket, String key) {
        log.info("Calling S3 DeleteObject source=s3://{}/{}", bucket, key);
        try {
            var response = s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build());
            log.info("S3 DeleteObject succeeded source=s3://{}/{} requestId={}",
                    bucket, key,
                    response == null || response.responseMetadata() == null
                            ? null
                            : response.responseMetadata().requestId());
        } catch (S3Exception exception) {
            int status = exception.statusCode();
            logS3Failure("DeleteObject", bucket, key, null, null, exception);
            if (status == 403) {
                String guidance = "Access denied (403) deleting source object " + bucket + "/" + key
                        + ". Possible causes: missing s3:DeleteObject permission, bucket policy/ACL denying delete, or object lock/retention."
                        + " Verify IAM policies and bucket settings. RequestId=" + exception.requestId();
                throw new S3ReconciliationException(guidance, exception);
            }
            throw new S3ReconciliationException("Failed to delete source object " + bucket + "/" + key
                    + " (status=" + status + ", requestId=" + exception.requestId() + "): " + exception.getMessage(), exception);
        }
    }

    private HeadObjectResponse readObjectMetadata(String bucket, String key, String context) {
        try {
            log.info("Calling S3 GetObject {} bucket={} key={}; response body will be closed after reading headers",
                    context, bucket, key);
            GetObjectRequest getReq = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            try (ResponseInputStream<GetObjectResponse> responseStream = s3Client.getObject(getReq)) {
                GetObjectResponse getResp = responseStream.response();
                HeadObjectResponse metadata = HeadObjectResponse.builder()
                        .contentLength(getResp.contentLength())
                        .eTag(getResp.eTag())
                        .build();
                log.info("S3 GetObject headers received for {} bucket={} key={}: eTag={}, contentLength={}; closing body stream",
                        context, bucket, key, getResp.eTag(), getResp.contentLength());
                return metadata;
            }
        } catch (S3Exception exception) {
            logS3Failure("GetObject (headers only, context=" + context + ")",
                    bucket, key, null, null, exception);
            throw exception;
        } catch (java.io.IOException exception) {
            throw new S3ReconciliationException(
                    "Failed to close GetObject response stream for s3://" + bucket + "/" + key,
                    exception);
        }
    }

    private void logS3Failure(
            String operation,
            String bucket,
            String key,
            String destinationBucket,
            String destinationKey,
            S3Exception exception) {
        String errorCode = exception.awsErrorDetails() == null
                ? null
                : exception.awsErrorDetails().errorCode();
        String errorMessage = exception.awsErrorDetails() == null
                ? exception.getMessage()
                : exception.awsErrorDetails().errorMessage();
        log.error(
                "S3 {} failed source=s3://{}/{} destination={} status={} errorCode={} requestId={} extendedRequestId={} message={}",
                operation,
                bucket,
                key,
                destinationBucket == null ? "<none>" : "s3://" + destinationBucket + "/" + destinationKey,
                exception.statusCode(),
                errorCode,
                exception.requestId(),
                exception.extendedRequestId(),
                errorMessage,
                exception);
    }

    private String backupKey(String bucket, String key) {
        String normalizedKey = key.replaceAll("^/+", "");
        // Store backups under <backup-bucket>/<source-bucket>/<original/path>
        // If the configured backup bucket equals the source bucket, avoid duplicating
        // the bucket name in the object key (results like bucket/bucket/...).
        String backupBucket = properties.backupBucket();
        if (backupBucket != null && backupBucket.equals(bucket)) {
            log.warn("Configured backup-bucket '{}' is the same as source bucket '{}'; using original object key for backup to avoid duplicate bucket prefix", backupBucket, bucket);
            return normalizedKey;
        }
        return bucket + "/" + normalizedKey;
    }
}
