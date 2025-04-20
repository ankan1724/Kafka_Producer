package com.kafka.producer.KafkaProducer.service;

import com.kafka.producer.KafkaProducer.PaymentMessages;
import com.kafka.producer.KafkaProducer.Status;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;


import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

@Service
@Slf4j
public class Publisher {

    @Autowired
    private KafkaTemplate<String, Message<PaymentMessages>> sslTemplate;

    @Autowired
    private KafkaTemplate<String, Object> nonSslTemplate;

    @Autowired
    private PaymentMessages messages;

    @Scheduled(fixedDelay = 30000)
    public void sendMessage() {
        messages.setTransactionId(UUID.randomUUID().toString());
        messages.setTimestamp(new Date(System.currentTimeMillis()));
        messages.setAmount(Math.random());
        messages.setCurrency("$");
        messages.setPayee("John");
        messages.setPayer("Amanda");
        messages.setStatus(Status.values()[ThreadLocalRandom.current().nextInt(Status.values().length)]);
        if (sslTemplate == null) {
            log.error("sslTemplate is null");
        } else {
            Message<PaymentMessages> message = MessageBuilder.withPayload(messages).setHeader(KafkaHeaders.TOPIC, "payment_EOD_data").setHeader("eventType", "PAYMENT_DATA").setHeader("tenantId", "abc123").build();
            CompletableFuture<SendResult<String, Message<PaymentMessages>>> futureSsl = sslTemplate.send("payment_EOD_data", "Payment_data", message);

            futureSsl.whenComplete((result, ex) -> {
                if (ex == null && result != null) {
                    if (result.getProducerRecord().value() == message)
                        log.info("SSL message sent with acks = " + (result.getProducerRecord().value() == message));
                } else {
                    log.warn("Unable to send SSL message. " + ex.getMessage());
                }
            });

        }
    }
}


