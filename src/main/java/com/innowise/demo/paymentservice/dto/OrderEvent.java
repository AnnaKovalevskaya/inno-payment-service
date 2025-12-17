package com.innowise.demo.paymentservice.dto;

import lombok.Data;
import java.time.LocalDateTime;

@Data
public class OrderEvent {
    private String orderId;
    private String userId;
    private LocalDateTime timestamp;
    private Double amount;
}
