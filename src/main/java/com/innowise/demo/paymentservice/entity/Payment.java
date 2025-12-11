package com.innowise.demo.paymentservice.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.time.LocalDateTime;

@Document(collection = "payments")
@Data
public class Payment {
    @Id
    private String id;
    private String orderId;
    private String userId;
    private String status;
    private LocalDateTime timestamp;
    private double paymentAmount;
}