package com.innowise.demo.paymentservice.repository;

import com.innowise.demo.paymentservice.entity.Payment;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PaymentRepository extends MongoRepository<Payment, String> {

    @Query("{ 'orderId': { $eq: ?0 } }")
    List<Payment> findByOrderId(Long orderId);

    @Query("{ 'userId': { $eq: ?0 } }")
    List<Payment> findByUserId(Long userId);

    List<Payment> findByStatus(String status);
}