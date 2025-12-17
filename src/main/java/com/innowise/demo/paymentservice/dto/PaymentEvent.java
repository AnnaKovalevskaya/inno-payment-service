package com.innowise.demo.paymentservice.dto;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class PaymentEvent {
    private String paymentId;
    private String orderId;
    private String status;
    private LocalDateTime timestamp;
}
