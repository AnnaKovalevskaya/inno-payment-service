package com.innowise.demo.paymentservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.time.LocalDateTime;
import lombok.Data;

@Data
public class PaymentDto {
    private String id;

    @NotBlank(message = "Order ID cannot be blank")
    @Size(max = 50, message = "Order ID must be less than 50 characters")
    private String orderId;

    @NotBlank(message = "User ID cannot be blank")
    @Size(max = 50, message = "User ID must be less than 50 characters")
    private String userId;

    private String status;

    @NotNull(message = "Timestamp cannot be null")
    private LocalDateTime timestamp;

    @NotNull(message = "Payment amount cannot be null")
    @Positive(message = "Payment amount must be positive")
    private Double paymentAmount;
}