package com.innowise.demo.paymentservice.service;

import com.innowise.demo.paymentservice.dto.PaymentEvent;
import com.innowise.demo.paymentservice.entity.Payment;
import com.innowise.demo.paymentservice.repository.PaymentRepository;
import com.innowise.demo.paymentservice.dto.PaymentDto;
import com.innowise.demo.paymentservice.kafka.PaymentKafkaProducer;
import com.innowise.demo.paymentservice.mapper.PaymentMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private static final Logger logger = LoggerFactory.getLogger(PaymentService.class);
    private static final Random RANDOM = new Random();

    private final PaymentRepository paymentRepository;
    private final PaymentMapper paymentMapper;
    private final MongoTemplate mongoTemplate;
    private final PaymentKafkaProducer paymentKafkaProducer;

    private final WebClient webClient = WebClient.create();

    public PaymentDto createPayment(PaymentDto paymentDto) {
        logger.info("Creating payment for orderId: {}", paymentDto.getOrderId());

        Payment payment = new Payment();
        payment.setOrderId(paymentDto.getOrderId());
        payment.setUserId(paymentDto.getUserId());
        payment.setStatus("PENDING");
        payment.setTimestamp(paymentDto.getTimestamp() != null ? paymentDto.getTimestamp() : LocalDateTime.now());
        payment.setPaymentAmount(paymentDto.getPaymentAmount());

        Payment savedPayment = paymentRepository.save(payment);
        logger.info("Payment saved with ID: {}", savedPayment.getId());

        CompletableFuture.runAsync(() -> {
            processPaymentAsync(savedPayment.getId());
        });

        PaymentDto result = new PaymentDto();
        result.setId(savedPayment.getId());
        result.setOrderId(savedPayment.getOrderId());
        result.setUserId(savedPayment.getUserId());
        result.setStatus(savedPayment.getStatus());
        result.setTimestamp(savedPayment.getTimestamp());
        result.setPaymentAmount(savedPayment.getPaymentAmount());

        return result;
    }

    private void processPaymentAsync(String paymentId) {
        logger.info("Processing payment asynchronously: {}", paymentId);

        try {
            int delaySeconds = 3 + RANDOM.nextInt(3);
            logger.info("Simulating payment processing for {} seconds...", delaySeconds);
            Thread.sleep(delaySeconds * 1000L);

            String finalStatus = generateRandomPaymentStatus();
            logger.info("Generated status for payment {}: {}", paymentId, finalStatus);

            Payment payment = paymentRepository.findById(paymentId)
                    .orElseThrow(() -> new RuntimeException("Payment not found: " + paymentId));

            payment.setStatus(finalStatus);
            Payment updatedPayment = paymentRepository.save(payment);

            PaymentEvent paymentEvent = new PaymentEvent();
            paymentEvent.setPaymentId(updatedPayment.getId());
            paymentEvent.setOrderId(updatedPayment.getOrderId().toString());
            paymentEvent.setStatus(updatedPayment.getStatus());
            paymentEvent.setTimestamp(updatedPayment.getTimestamp());

            paymentKafkaProducer.sendPaymentEvent(paymentEvent);

            logger.info("Payment {} processed with status: {}", paymentId, finalStatus);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Payment processing interrupted for {}", paymentId, e);
        } catch (Exception e) {
            logger.error("Error processing payment {}", paymentId, e);
        }
    }

    private String generateRandomPaymentStatus() {
        int randomNumber = RANDOM.nextInt(100);

        if (randomNumber < 70) {
            return "SUCCESS";
        } else {
            return "FAILED";
        }
    }

    public List<PaymentDto> getAllPayments() {
        return paymentRepository.findAll().stream()
                .map(paymentMapper::toDto)
                .collect(Collectors.toList());
    }

    public List<PaymentDto> getPaymentsByOrderId(Long orderId) {
        return paymentRepository.findByOrderId(orderId).stream()
                .map(paymentMapper::toDto)
                .collect(Collectors.toList());
    }

    public List<PaymentDto> getPaymentsByUserId(Long userId) {
        return paymentRepository.findByUserId(userId).stream()
                .map(paymentMapper::toDto)
                .collect(Collectors.toList());
    }

    public List<PaymentDto> getPaymentsByStatus(String status) {
        return paymentRepository.findByStatus(status).stream()
                .map(paymentMapper::toDto)
                .collect(Collectors.toList());
    }

    public double getTotalPaymentAmountForPeriod(LocalDateTime startDate, LocalDateTime endDate) {
        logger.info("Calculating total for period: {} to {}", startDate, endDate);

        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("timestamp").gte(startDate).lte(endDate)),
                Aggregation.group().sum("paymentAmount").as("total")
        );

        AggregationResults<TotalResult> results = mongoTemplate.aggregate(aggregation, "payments", TotalResult.class);
        TotalResult result = results.getUniqueMappedResult();

        double total = result != null ? result.getTotal() : 0.0;
        logger.info("Total calculated: {}", total);

        return total;
    }

    public static class TotalResult {
        private double total;
        public double getTotal() { return total; }
        public void setTotal(double total) { this.total = total; }
    }
}