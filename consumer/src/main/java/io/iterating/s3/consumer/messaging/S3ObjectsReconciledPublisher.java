package io.iterating.s3.consumer.messaging;

public interface S3ObjectsReconciledPublisher {

    void publish(S3ObjectsReconciled event) throws Exception;
}
