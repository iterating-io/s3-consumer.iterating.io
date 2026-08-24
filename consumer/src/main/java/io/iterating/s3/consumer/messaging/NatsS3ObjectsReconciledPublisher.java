package io.iterating.s3.consumer.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.nats.client.Connection;
import io.nats.client.JetStream;

public class NatsS3ObjectsReconciledPublisher implements S3ObjectsReconciledPublisher {

    public static final String SUBJECT = "s3.objects.reconciled";

    private final JetStream jetStream;
    private final ObjectMapper objectMapper;

    public NatsS3ObjectsReconciledPublisher(Connection connection, ObjectMapper objectMapper) throws java.io.IOException {
        this.jetStream = connection.jetStream();
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(S3ObjectsReconciled event) throws Exception {
        jetStream.publish(SUBJECT, objectMapper.writeValueAsBytes(event));
    }
}
