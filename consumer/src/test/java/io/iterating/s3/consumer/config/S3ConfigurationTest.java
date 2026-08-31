package io.iterating.s3.consumer.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

class S3ConfigurationTest {

    @Test
    void createsPathStyleClientWhenEndpointOverrideIsConfigured() {
        S3ReconciliationProperties properties = new S3ReconciliationProperties(
                "backup-dev", "us-east-1", "https://s3.iterating.io");
        var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("test-key", "test-secret"));

        try (var client = new S3Configuration().s3Client(properties, credentials)) {
            assertNotNull(client);
        }
    }
}
