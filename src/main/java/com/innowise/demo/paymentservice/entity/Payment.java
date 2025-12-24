package com.innowise.demo.paymentservice.entity;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.LocalDateTime;

@Document(collection = "payments")
@Data
public class Payment {
    @Id
    private String id;

    @Field("orderId")
    @JsonSerialize(using = ToStringSerializer.class)
    private Long orderId;

    @Field("userId")
    @JsonSerialize(using = ToStringSerializer.class)
    private Long userId;

    @Field("status")
    private String status;

    @Field("timestamp")
    private LocalDateTime timestamp;

    @Field("paymentAmount")
    private Double paymentAmount;
}