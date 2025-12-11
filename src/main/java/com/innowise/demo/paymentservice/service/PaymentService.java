package com.innowise.demo.paymentservice.service;

import com.innowise.demo.paymentservice.entity.Payment;
import com.innowise.demo.paymentservice.repository.PaymentRepository;
import com.innowise.demo.paymentservice.dto.PaymentDto;
import com.innowise.demo.paymentservice.mapper.PaymentMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class PaymentService {

    private static final Logger logger = LoggerFactory.getLogger(PaymentService.class);

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private PaymentMapper paymentMapper;

    @Autowired
    private MongoTemplate mongoTemplate;

    private final WebClient webClient = WebClient.create();

    public PaymentDto createPayment(PaymentDto paymentDto) {
        Payment payment = paymentMapper.toEntity(paymentDto);
        String status = generateStatusFromApi();
        payment.setStatus(status);
        Payment savedPayment = paymentRepository.save(payment);
        return paymentMapper.toDto(savedPayment);
    }

    public List<PaymentDto> getAllPayments() {
        return paymentRepository.findAll().stream()
                .map(paymentMapper::toDto)
                .collect(Collectors.toList());
    }

    public List<PaymentDto> getPaymentsByOrderId(String orderId) {
        return paymentRepository.findByOrderId(orderId).stream()
                .map(paymentMapper::toDto)
                .collect(Collectors.toList());
    }

    public List<PaymentDto> getPaymentsByUserId(String userId) {
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

    private String generateStatusFromApi() {
        try {
            String response = webClient.get()
                    .uri("https://www.random.org/integers/?num=1&min=1&max=100&col=1&base=10&format=plain&rnd=new")
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
            int randomNumber = Integer.parseInt(response.trim());
            logger.info("Generated random number: {}", randomNumber);
            return (randomNumber % 2 == 0) ? "SUCCESS" : "FAILED";
        } catch (Exception e) {
            logger.error("Error calling external API, defaulting to FAILED", e);
            return "FAILED";
        }
    }

    public static class TotalResult {
        private double total;
        public double getTotal() { return total; }
        public void setTotal(double total) { this.total = total; }
    }
}