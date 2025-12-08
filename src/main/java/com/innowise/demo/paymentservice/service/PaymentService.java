package com.innowise.demo.paymentservice.service;

import com.innowise.demo.paymentservice.entity.Payment;
import com.innowise.demo.paymentservice.repository.PaymentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;


@Service
public class PaymentService {

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    public Payment createPayment(Payment payment) {
        return paymentRepository.save(payment);
    }

    public List<Payment> getAllPayments() {
        return paymentRepository.findAll();
    }

    public List<Payment> getPaymentsByOrderId(String orderId) {
        return paymentRepository.findByOrderId(orderId);
    }

    public List<Payment> getPaymentsByUserId(String userId) {
        return paymentRepository.findByUserId(userId);
    }

    public List<Payment> getPaymentsByStatus(String status) {
        return paymentRepository.findByStatus(status);
    }

    public double getTotalPaymentAmountForPeriod(LocalDateTime startDate, LocalDateTime endDate) {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.match(Criteria.where("timestamp").gte(startDate).lte(endDate)),
                Aggregation.group().sum("paymentAmount").as("total")
        );
        AggregationResults<TotalResult> results = mongoTemplate.aggregate(aggregation, "payments", TotalResult.class);
        TotalResult result = results.getUniqueMappedResult();
        return result != null ? result.getTotal() : 0.0;
    }
    public static class TotalResult {
        private double total;
        public double getTotal() { return total; }
        public void setTotal(double total) { this.total = total; }
    }
}
