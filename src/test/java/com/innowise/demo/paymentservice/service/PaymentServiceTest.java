package com.innowise.demo.paymentservice.service;

import com.innowise.demo.paymentservice.dto.PaymentDto;
import com.innowise.demo.paymentservice.entity.Payment;
import com.innowise.demo.paymentservice.kafka.PaymentKafkaProducer;
import com.innowise.demo.paymentservice.mapper.PaymentMapper;
import com.innowise.demo.paymentservice.repository.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private PaymentMapper paymentMapper;

    @Mock
    private MongoTemplate mongoTemplate;

    @Mock
    private PaymentKafkaProducer paymentKafkaProducer;

    @InjectMocks
    private PaymentService paymentService;

    private PaymentDto paymentDto;
    private Payment payment;
    private LocalDateTime now;

    @BeforeEach
    void setUp() {
        now = LocalDateTime.now();

        paymentDto = new PaymentDto();
        paymentDto.setOrderId(1L);
        paymentDto.setUserId(1L);
        paymentDto.setPaymentAmount(100.0);
        paymentDto.setTimestamp(now);

        payment = new Payment();
        payment.setId("test-id");
        payment.setOrderId(1L);
        payment.setUserId(1L);
        payment.setPaymentAmount(100.0);
        payment.setTimestamp(now);
        payment.setStatus("PENDING");
    }

    @Test
    void createPayment_ShouldReturnPaymentDto() {
        when(paymentRepository.save(any(Payment.class))).thenReturn(payment);

        PaymentDto result = paymentService.createPayment(paymentDto);

        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo("test-id");
        assertThat(result.getOrderId()).isEqualTo(1L);
        verify(paymentRepository, times(1)).save(any(Payment.class));
    }

    @Test
    void createPayment_WithNullTimestamp_ShouldSetCurrentTimestamp() {
        paymentDto.setTimestamp(null);
        Payment savedPayment = new Payment();
        savedPayment.setId("test-id");
        savedPayment.setTimestamp(now);

        when(paymentRepository.save(any(Payment.class))).thenReturn(savedPayment);

        PaymentDto result = paymentService.createPayment(paymentDto);

        assertThat(result.getTimestamp()).isNotNull();
    }

    @Test
    void getAllPayments_ShouldReturnList() {
        List<Payment> payments = Arrays.asList(payment);
        when(paymentRepository.findAll()).thenReturn(payments);
        when(paymentMapper.toDto(payment)).thenReturn(paymentDto);

        List<PaymentDto> result = paymentService.getAllPayments();

        assertThat(result).hasSize(1);
        verify(paymentRepository, times(1)).findAll();
        verify(paymentMapper, times(1)).toDto(payment);
    }

    @Test
    void getPaymentsByOrderId_ShouldReturnFilteredList() {
        List<Payment> payments = Arrays.asList(payment);
        when(paymentRepository.findByOrderId(1L)).thenReturn(payments);
        when(paymentMapper.toDto(payment)).thenReturn(paymentDto);

        List<PaymentDto> result = paymentService.getPaymentsByOrderId(1L);

        assertThat(result).hasSize(1);
        verify(paymentRepository, times(1)).findByOrderId(1L);
    }

    @Test
    void getPaymentsByUserId_ShouldReturnFilteredList() {
        List<Payment> payments = Arrays.asList(payment);
        when(paymentRepository.findByUserId(1L)).thenReturn(payments);
        when(paymentMapper.toDto(payment)).thenReturn(paymentDto);

        List<PaymentDto> result = paymentService.getPaymentsByUserId(1L);

        assertThat(result).hasSize(1);
        verify(paymentRepository, times(1)).findByUserId(1L);
    }

    @Test
    void getPaymentsByStatus_ShouldReturnFilteredList() {
        List<Payment> payments = Arrays.asList(payment);
        when(paymentRepository.findByStatus("PENDING")).thenReturn(payments);
        when(paymentMapper.toDto(payment)).thenReturn(paymentDto);

        List<PaymentDto> result = paymentService.getPaymentsByStatus("PENDING");

        assertThat(result).hasSize(1);
        verify(paymentRepository, times(1)).findByStatus("PENDING");
    }

    @Test
    void getTotalPaymentAmountForPeriod_ShouldReturnCorrectTotal() {
        LocalDateTime start = now.minusDays(1);
        LocalDateTime end = now;

        PaymentService.TotalResult totalResult = new PaymentService.TotalResult();
        totalResult.setTotal(200.0);

        AggregationResults<PaymentService.TotalResult> mockResults = mock(AggregationResults.class);
        when(mongoTemplate.aggregate(any(Aggregation.class), anyString(), eq(PaymentService.TotalResult.class)))
                .thenReturn(mockResults);
        when(mockResults.getUniqueMappedResult()).thenReturn(totalResult);

        double result = paymentService.getTotalPaymentAmountForPeriod(start, end);

        assertThat(result).isEqualTo(200.0);
        verify(mongoTemplate, times(1))
                .aggregate(any(Aggregation.class), eq("payments"), eq(PaymentService.TotalResult.class));
    }

    @Test
    void getTotalPaymentAmountForPeriod_WhenNoResults_ShouldReturnZero() {
        LocalDateTime start = now.minusDays(1);
        LocalDateTime end = now;

        AggregationResults<PaymentService.TotalResult> mockResults = mock(AggregationResults.class);
        when(mongoTemplate.aggregate(any(Aggregation.class), anyString(), eq(PaymentService.TotalResult.class)))
                .thenReturn(mockResults);
        when(mockResults.getUniqueMappedResult()).thenReturn(null);

        double result = paymentService.getTotalPaymentAmountForPeriod(start, end);

        assertThat(result).isEqualTo(0.0);
    }

    @Test
    void findPaymentById_ShouldReturnPayment() {
        when(paymentRepository.findById("test-id")).thenReturn(Optional.of(payment));

        Optional<Payment> result = paymentRepository.findById("test-id");

        assertThat(result).isPresent();
        assertThat(result.get().getId()).isEqualTo("test-id");
    }
}