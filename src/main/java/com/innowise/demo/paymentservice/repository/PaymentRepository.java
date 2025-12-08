package com.innowise.demo.paymentservice.repository;

import com.innowise.demo.paymentservice.entity.Payment;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PaymentRepository extends MongoRepository<Payment, String> {
    List<Payment> findByOrderId(String orderId);

    List<Payment> findByUserId(String userId);

    List<Payment> findByStatus(String status);
}
