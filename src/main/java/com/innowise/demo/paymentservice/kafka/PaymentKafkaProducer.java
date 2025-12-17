package com.innowise.demo.paymentservice.kafka;

import com.innowise.demo.paymentservice.dto.PaymentEvent;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PaymentKafkaProducer {

    private static final Logger logger = LoggerFactory.getLogger(PaymentKafkaProducer.class);
    private static final String TOPIC = "create-payment";

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    public void sendPaymentEvent(PaymentEvent event) {
        logger.info("Sending payment event: {}", event);

        kafkaTemplate.send(TOPIC, event);
    }
}
