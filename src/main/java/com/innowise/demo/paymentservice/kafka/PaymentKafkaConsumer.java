package com.innowise.demo.paymentservice.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.innowise.demo.paymentservice.dto.OrderEvent;
import com.innowise.demo.paymentservice.dto.PaymentDto;
import com.innowise.demo.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PaymentKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PaymentKafkaConsumer.class);

    private final PaymentService paymentService;
    private final PaymentKafkaProducer paymentKafkaProducer;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "create-order", groupId = "payment-group")
    public void handleCreateOrder(String message) {
        logger.info("=== KAFKA CONSUMER: Received order event ===");
        logger.info("Message: {}", message);

        try {
            OrderEvent orderEvent = objectMapper.readValue(message, OrderEvent.class);

            logger.info("Parsed order event - orderId: {}, userId: {}, amount: {}",
                    orderEvent.getOrderId(), orderEvent.getUserId(), orderEvent.getAmount());

            PaymentDto paymentDto = new PaymentDto();
            paymentDto.setOrderId(Long.valueOf(orderEvent.getOrderId()));
            paymentDto.setUserId(Long.parseLong(orderEvent.getUserId()));
            paymentDto.setTimestamp(orderEvent.getTimestamp());
            paymentDto.setPaymentAmount(orderEvent.getAmount());
            paymentDto.setStatus("PENDING");

            PaymentDto createdPayment = paymentService.createPayment(paymentDto);

            logger.info("Created payment with ID: {}", createdPayment.getId());

        } catch (Exception e) {
            logger.error("Failed to process order event: {}", message, e);
        }
    }
}