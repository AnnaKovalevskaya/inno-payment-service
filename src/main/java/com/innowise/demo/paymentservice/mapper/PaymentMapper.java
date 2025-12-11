package com.innowise.demo.paymentservice.mapper;

import com.innowise.demo.paymentservice.dto.PaymentDto;
import com.innowise.demo.paymentservice.entity.Payment;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface PaymentMapper {
    PaymentDto toDto(Payment entity);
    Payment toEntity(PaymentDto dto);
}