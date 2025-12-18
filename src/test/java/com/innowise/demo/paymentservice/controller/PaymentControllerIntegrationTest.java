package com.innowise.demo.paymentservice.controller;

import com.innowise.demo.paymentservice.AbstractIntegrationTest;
import com.innowise.demo.paymentservice.dto.PaymentDto;
import com.innowise.demo.paymentservice.entity.Payment;
import com.innowise.demo.paymentservice.repository.PaymentRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@AutoConfigureMockMvc
class PaymentControllerIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    @AfterEach
    void cleanup() {
        paymentRepository.deleteAll();
    }

    @Test
    void createPayment_ShouldReturnCreatedPayment() throws Exception {
        PaymentDto paymentDto = new PaymentDto();
        paymentDto.setOrderId(1L);
        paymentDto.setUserId(1L);
        paymentDto.setPaymentAmount(100.0);
        paymentDto.setTimestamp(LocalDateTime.now());
        paymentDto.setStatus("PENDING");

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(paymentDto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.orderId").value(1))
                .andExpect(jsonPath("$.userId").value(1))
                .andExpect(jsonPath("$.status").value("PENDING"));

        List<Payment> payments = paymentRepository.findAll();
        assertThat(payments).hasSize(1);
        assertThat(payments.get(0).getOrderId()).isEqualTo(1L);
    }

    @Test
    void createPayment_WithInvalidData_ShouldReturnBadRequest() throws Exception {
        PaymentDto paymentDto = new PaymentDto();

        mockMvc.perform(post("/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(paymentDto)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getAllPayments_ShouldReturnPayments() throws Exception {
        Payment payment = new Payment();
        payment.setOrderId(1L);
        payment.setUserId(1L);
        payment.setPaymentAmount(100.0);
        payment.setTimestamp(LocalDateTime.now());
        payment.setStatus("SUCCESS");
        paymentRepository.save(payment);

        mockMvc.perform(get("/payments"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].orderId").value(1))
                .andExpect(jsonPath("$[0].userId").value(1))
                .andExpect(jsonPath("$[0].status").value("SUCCESS"));
    }

    @Test
    void getPaymentsByOrderId_ShouldReturnFiltered() throws Exception {
        Payment payment1 = new Payment();
        payment1.setOrderId(1L);
        payment1.setUserId(1L);
        payment1.setPaymentAmount(100.0);
        payment1.setTimestamp(LocalDateTime.now());
        payment1.setStatus("SUCCESS");

        Payment payment2 = new Payment();
        payment2.setOrderId(2L);
        payment2.setUserId(1L);
        payment2.setPaymentAmount(200.0);
        payment2.setTimestamp(LocalDateTime.now());
        payment2.setStatus("SUCCESS");

        paymentRepository.saveAll(List.of(payment1, payment2));

        mockMvc.perform(get("/payments/order/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].orderId").value(1))
                .andExpect(jsonPath("$.length()").value(1));
    }

    @Test
    void getTotalPaymentAmountForPeriod_ShouldCalculateCorrectly() throws Exception {
        Payment payment = new Payment();
        payment.setOrderId(1L);
        payment.setUserId(1L);
        payment.setPaymentAmount(150.0);
        payment.setTimestamp(LocalDateTime.now());
        payment.setStatus("SUCCESS");
        paymentRepository.save(payment);

        LocalDateTime now = LocalDateTime.now();
        String startDate = now.minusDays(1).toString();
        String endDate = now.plusDays(1).toString();

        mockMvc.perform(get("/payments/total")
                        .param("startDate", startDate)
                        .param("endDate", endDate))
                .andExpect(status().isOk())
                .andExpect(content().string("150.0"));
    }
}