package com.innowise.demo.paymentservice;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.testcontainers.containers.wait.strategy.Wait.forListeningPort;

@Testcontainers
@SpringBootTest
@ActiveProfiles("test")
public abstract class AbstractIntegrationTest {

    static final MongoDBContainer mongoDBContainer = new MongoDBContainer(
            DockerImageName.parse("mongo:6.0")
    ).withExposedPorts(27017)
     .waitingFor(forListeningPort());

    static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
    );

    @BeforeAll
    static void beforeAll() {
        mongoDBContainer.start();
        kafkaContainer.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.host", mongoDBContainer::getHost);
        registry.add("spring.data.mongodb.port", () -> mongoDBContainer.getMappedPort(27017));
        registry.add("spring.data.mongodb.database", () -> "test_payments");
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("wiremock.server.port", () -> 8081);
    }
}
