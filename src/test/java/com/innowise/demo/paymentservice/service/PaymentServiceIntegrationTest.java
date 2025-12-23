package com.innowise.demo.paymentservice.service;

import com.innowise.demo.paymentservice.AbstractIntegrationTest;
import com.innowise.demo.paymentservice.dto.PaymentDto;
import com.innowise.demo.paymentservice.entity.Payment;
import com.innowise.demo.paymentservice.repository.PaymentRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@ActiveProfiles("test")
public class PaymentServiceIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private PaymentService paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAll();
    }

    @AfterEach
    void cleanup() {
        paymentRepository.deleteAll();
    }

    @Test
    void createPayment_ShouldSaveToMongoAndPublishToKafka() {
        PaymentDto paymentDto = new PaymentDto();
        paymentDto.setOrderId(1L);
        paymentDto.setUserId(1L);
        paymentDto.setPaymentAmount(100.0);
        paymentDto.setTimestamp(LocalDateTime.now());

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group-payment-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "com.innowise.demo.paymentservice.dto");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.innowise.demo.paymentservice.dto.PaymentDto");
        consumerProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");

        Consumer<String, Object> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("payment-events"));

        try {
            PaymentDto result = paymentService.createPayment(paymentDto);

            List<Payment> payments = paymentRepository.findAll();
            assertThat(payments).hasSize(1);
            
            Payment savedPayment = payments.get(0);
            assertThat(savedPayment.getOrderId()).isEqualTo(1L);
            assertThat(savedPayment.getUserId()).isEqualTo(1L);
            assertThat(savedPayment.getPaymentAmount()).isEqualTo(100.0);
            assertThat(savedPayment.getStatus()).isEqualTo("PENDING");

            await().atMost(10, java.util.concurrent.TimeUnit.SECONDS).untilAsserted(() -> {
                Payment updatedPayment = paymentRepository.findById(result.getId()).orElseThrow();
                assertThat(updatedPayment.getStatus()).isNotNull();
            });

            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(5));
            assertThat(records).isNotEmpty();

            boolean foundPaymentEvent = false;
            for (ConsumerRecord<String, Object> record : records) {
                System.out.println("Kafka message - Topic: " + record.topic() + ", Value: " + record.value());
                if (record.value() != null && record.value().toString().contains("orderId")) {
                    foundPaymentEvent = true;
                }
            }
            assertThat(foundPaymentEvent).isTrue();

        } finally {
            consumer.close();
        }
    }

    @Test
    void createPayment_ShouldSaveToMongo() {
        PaymentDto paymentDto = new PaymentDto();
        paymentDto.setOrderId(2L);
        paymentDto.setUserId(2L);
        paymentDto.setPaymentAmount(200.0);
        paymentDto.setTimestamp(LocalDateTime.now());

        PaymentDto result = paymentService.createPayment(paymentDto);

        List<Payment> payments = paymentRepository.findAll();
        assertThat(payments).hasSize(1);
        
        Payment savedPayment = payments.get(0);
        assertThat(savedPayment.getOrderId()).isEqualTo(2L);
        assertThat(savedPayment.getUserId()).isEqualTo(2L);
        assertThat(savedPayment.getPaymentAmount()).isEqualTo(200.0);
        assertThat(savedPayment.getStatus()).isEqualTo("PENDING");
        
        assertThat(result.getId()).isNotNull();
        assertThat(result.getOrderId()).isEqualTo(2L);
    }

    @Test
    void getAllPayments_ShouldReturnAllPayments() {
        Payment payment1 = new Payment();
        payment1.setOrderId(101L);
        payment1.setUserId(1L);
        payment1.setPaymentAmount(100.0);
        payment1.setTimestamp(LocalDateTime.now());
        payment1.setStatus("SUCCESS");

        Payment payment2 = new Payment();
        payment2.setOrderId(102L);
        payment2.setUserId(2L);
        payment2.setPaymentAmount(200.0);
        payment2.setTimestamp(LocalDateTime.now());
        payment2.setStatus("FAILED");

        paymentRepository.saveAll(List.of(payment1, payment2));

        List<PaymentDto> result = paymentService.getAllPayments();

        assertThat(result).hasSize(2);
        
        assertThat(result)
            .extracting(PaymentDto::getOrderId)
            .containsExactlyInAnyOrder(101L, 102L);
    }

    @Test
    void getPaymentsByOrderId_ShouldReturnFilteredPayments() {
        Payment payment1 = new Payment();
        payment1.setOrderId(201L);
        payment1.setUserId(1L);
        payment1.setPaymentAmount(100.0);
        payment1.setTimestamp(LocalDateTime.now());
        payment1.setStatus("SUCCESS");

        Payment payment2 = new Payment();
        payment2.setOrderId(202L);
        payment2.setUserId(1L);
        payment2.setPaymentAmount(200.0);
        payment2.setTimestamp(LocalDateTime.now());
        payment2.setStatus("SUCCESS");

        paymentRepository.saveAll(List.of(payment1, payment2));

        List<PaymentDto> result = paymentService.getPaymentsByOrderId(201L);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getOrderId()).isEqualTo(201L);
        assertThat(result.get(0).getPaymentAmount()).isEqualTo(100.0);
    }

    @Test
    void getTotalPaymentAmountForPeriod_ShouldCalculateCorrectly() {
        LocalDateTime now = LocalDateTime.now();

        Payment payment1 = new Payment();
        payment1.setOrderId(301L);
        payment1.setUserId(1L);
        payment1.setPaymentAmount(100.0);
        payment1.setTimestamp(now.minusHours(2));
        payment1.setStatus("SUCCESS");

        Payment payment2 = new Payment();
        payment2.setOrderId(302L);
        payment2.setUserId(2L);
        payment2.setPaymentAmount(200.0);
        payment2.setTimestamp(now.minusHours(1));
        payment2.setStatus("SUCCESS");

        Payment payment3 = new Payment();
        payment3.setOrderId(303L);
        payment3.setUserId(3L);
        payment3.setPaymentAmount(300.0);
        payment3.setTimestamp(now.plusHours(1));
        payment3.setStatus("SUCCESS");

        Payment payment4 = new Payment();
        payment4.setOrderId(304L);
        payment4.setUserId(4L);
        payment4.setPaymentAmount(150.0);
        payment4.setTimestamp(now.minusHours(3));
        payment4.setStatus("FAILED");

        paymentRepository.saveAll(List.of(payment1, payment2, payment3, payment4));

        double total = paymentService.getTotalPaymentAmountForPeriod(
                now.minusDays(1),
                now
        );

        assertThat(total).isEqualTo(300.0);
    }

    @Test
    void getPaymentsByStatus_ShouldReturnFilteredPayments() {
        Payment payment1 = new Payment();
        payment1.setOrderId(401L);
        payment1.setUserId(1L);
        payment1.setPaymentAmount(100.0);
        payment1.setTimestamp(LocalDateTime.now());
        payment1.setStatus("SUCCESS");

        Payment payment2 = new Payment();
        payment2.setOrderId(402L);
        payment2.setUserId(2L);
        payment2.setPaymentAmount(200.0);
        payment2.setTimestamp(LocalDateTime.now());
        payment2.setStatus("FAILED");

        Payment payment3 = new Payment();
        payment3.setOrderId(403L);
        payment3.setUserId(3L);
        payment3.setPaymentAmount(300.0);
        payment3.setTimestamp(LocalDateTime.now());
        payment3.setStatus("SUCCESS");

        paymentRepository.saveAll(List.of(payment1, payment2, payment3));

        List<PaymentDto> successPayments = paymentService.getPaymentsByStatus("SUCCESS");
        List<PaymentDto> failedPayments = paymentService.getPaymentsByStatus("FAILED");

        assertThat(successPayments).hasSize(2);
        assertThat(failedPayments).hasSize(1);
        
        assertThat(successPayments)
            .extracting(PaymentDto::getOrderId)
            .containsExactlyInAnyOrder(401L, 403L);
            
        assertThat(failedPayments.get(0).getOrderId()).isEqualTo(402L);
    }

    @Test
    void getPaymentById_ShouldReturnPayment() {
        Payment payment = new Payment();
        payment.setOrderId(501L);
        payment.setUserId(1L);
        payment.setPaymentAmount(500.0);
        payment.setTimestamp(LocalDateTime.now());
        payment.setStatus("SUCCESS");
        
        Payment savedPayment = paymentRepository.save(payment);

        PaymentDto result = paymentService.getPaymentById(savedPayment.getId());

        assertThat(result).isNotNull();
        assertThat(result.getOrderId()).isEqualTo(501L);
        assertThat(result.getPaymentAmount()).isEqualTo(500.0);
        assertThat(result.getStatus()).isEqualTo("SUCCESS");
    }
}
