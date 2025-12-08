package com.innowise.demo.paymentservice.controller;

import com.innowise.demo.paymentservice.entity.Payment;
import com.innowise.demo.paymentservice.service.PaymentService;
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
    public List<Payment> getAllPayments() {
        return paymentService.getAllPayments();
    }

    @PostMapping
    public Payment createPayment(@RequestBody Payment payment) {
        return paymentService.createPayment(payment);
    }

    @GetMapping("/order/{orderId}")
    public List<Payment> getPaymentsByOrderId(@PathVariable String orderId) {
        return paymentService.getPaymentsByOrderId(orderId);
    }

    @GetMapping("/user/{userId}")
    public List<Payment> getPaymentsByUserId(@PathVariable String userId) {
        return paymentService.getPaymentsByUserId(userId);
    }

    @GetMapping("/status/{status}")
    public List<Payment> getPaymentsByStatus(@PathVariable String status) {
        return paymentService.getPaymentsByStatus(status);
    }

    @GetMapping("/total")
    public double getTotalPaymentAmountForPeriod(
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime startDate,
                                                   @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime endDate) {
        return paymentService.getTotalPaymentAmountForPeriod(startDate, endDate);
    }
}
