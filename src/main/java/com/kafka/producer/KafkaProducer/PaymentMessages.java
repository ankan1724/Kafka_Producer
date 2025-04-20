package com.kafka.producer.KafkaProducer;

import com.fasterxml.jackson.annotation.JsonFormat;
import org.springframework.stereotype.Component;

import java.util.Date;


@Component
public class PaymentMessages {

    private String transactionId;
    private double amount;
    private String currency;
    private String payer;
    private String payee;

    private Status status;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
    private Date timestamp;

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getPayer() {
        return payer;
    }

    public void setPayer(String payer) {
        this.payer = payer;
    }

    public String getPayee() {
        return payee;
    }

    public void setPayee(String payee) {
        this.payee = payee;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public PaymentMessages(String transactionId, double amount, String currency, String payer, String payee, Status status, Date timestamp) {
        this.transactionId = transactionId;
        this.amount = amount;
        this.currency = currency;
        this.payer = payer;
        this.payee = payee;
        this.status = status;
        this.timestamp = timestamp;
    }

    public PaymentMessages() {
    }
}
