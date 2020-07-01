package com.hazelcast.jet.dto;

import java.util.Date;

public class Order {
    private String uid;
    private Double amount;
    private String id;
    private Date timestamp;

    public Order(String uid, Double amount, String id, Date timestamp) {
        this.uid = uid;
        this.amount = amount;
        this.id = id;
        this.timestamp = timestamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
}
