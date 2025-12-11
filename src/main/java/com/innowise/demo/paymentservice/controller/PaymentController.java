package com.innowise.demo.paymentservice.controller;

import com.innowise.demo.paymentservice.dto.PaymentDto;
import com.innowise.demo.paymentservice.service.PaymentService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    @Autowired
    private PaymentService paymentService;

    @GetMapping
    public List<PaymentDto> getAllPayments() {
        return paymentService.getAllPayments();
    }

    @PostMapping
    public PaymentDto createPayment(@Valid @RequestBody PaymentDto paymentDto) {
        return paymentService.createPayment(paymentDto);
    }

    @GetMapping("/order/{orderId}")
    public List<PaymentDto> getPaymentsByOrderId(@PathVariable String orderId) {
        return paymentService.getPaymentsByOrderId(orderId);
    }

    @GetMapping("/user/{userId}")
    public List<PaymentDto> getPaymentsByUserId(@PathVariable String userId) {
        return paymentService.getPaymentsByUserId(userId);
    }

    @GetMapping("/status/{status}")
    public List<PaymentDto> getPaymentsByStatus(@PathVariable String status) {
        return paymentService.getPaymentsByStatus(status);
    }

    @GetMapping("/total")
    public double getTotalPaymentAmountForPeriod(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startDate,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endDate) {
        return paymentService.getTotalPaymentAmountForPeriod(startDate, endDate);
    }
}