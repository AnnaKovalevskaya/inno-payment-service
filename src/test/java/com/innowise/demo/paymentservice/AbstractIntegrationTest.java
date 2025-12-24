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

    public static final MongoDBContainer mongoDBContainer = new MongoDBContainer(
            DockerImageName.parse("mongo:6.0")
    ).withExposedPorts(27017)
     .waitingFor(forListeningPort());

    public static final KafkaContainer kafkaContainer = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
    );

    @BeforeAll
    static void beforeAll() {
        mongoDBContainer.start();
        kafkaContainer.start();
        
        waitForContainer(mongoDBContainer, 27017);
        waitForContainer(kafkaContainer, 9092);
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        String mongoHost = mongoDBContainer.getHost();
        Integer mongoPort = mongoDBContainer.getMappedPort(27017);
        
        System.out.println("MongoDB host: " + mongoHost);
        System.out.println("MongoDB port: " + mongoPort);
        System.out.println("MongoDB connection: " + mongoHost + ":" + mongoPort);
        
        String mongoUri = String.format("mongodb://%s:%d/test_payments", mongoHost, mongoPort);
        registry.add("spring.data.mongodb.uri", () -> mongoUri);
        
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("wiremock.server.port", () -> 8081);
    }
    
    private static void waitForContainer(org.testcontainers.containers.GenericContainer<?> container, int port) {
        try {
            int mappedPort = container.getMappedPort(port);
            System.out.println("Waiting for container " + container.getContainerName() + 
                             " on port " + mappedPort + "...");
            
            Thread.sleep(2000);
        } catch (Exception e) {
            System.out.println("Error waiting for container: " + e.getMessage());
        }
    }
}
