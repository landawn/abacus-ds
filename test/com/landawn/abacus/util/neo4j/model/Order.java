package com.landawn.abacus.util.neo4j.model;

import java.util.Date;
import java.util.List;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

@NodeEntity(label = "Order")
public class Order {
    @GraphId
    private Long id;
    @Relationship(type = "CONTAINS", direction = Relationship.OUTGOING)
    private List<OrderItem> orderItemList;
    private double amount;
    @Relationship(type = "HAS", direction = Relationship.OUTGOING)
    private Address shippingAddress;
    private Date date;

    public Long getId() {
        return id;
    }

    public Order setId(Long id) {
        this.id = id;
        return this;
    }

    public List<OrderItem> getOrderItemList() {
        return orderItemList;
    }

    public Order setOrderItemList(List<OrderItem> orderItemList) {
        this.orderItemList = orderItemList;
        return this;
    }

    public double getAmount() {
        return amount;
    }

    public Order setAmount(double amount) {
        this.amount = amount;
        return this;
    }

    public Address getShippingAddress() {
        return shippingAddress;
    }

    public Order setShippingAddress(Address shippingAddress) {
        this.shippingAddress = shippingAddress;
        return this;
    }

    public Date getDate() {
        return date;
    }

    public Order setDate(Date date) {
        this.date = date;
        return this;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(amount);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + ((date == null) ? 0 : date.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((orderItemList == null) ? 0 : orderItemList.hashCode());
        result = prime * result + ((shippingAddress == null) ? 0 : shippingAddress.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Order other = (Order) obj;
        if (Double.doubleToLongBits(amount) != Double.doubleToLongBits(other.amount))
            return false;
        if (date == null) {
            if (other.date != null)
                return false;
        } else if (!date.equals(other.date))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (orderItemList == null) {
            if (other.orderItemList != null)
                return false;
        } else if (!orderItemList.equals(other.orderItemList))
            return false;
        if (shippingAddress == null) {
            if (other.shippingAddress != null)
                return false;
        } else if (!shippingAddress.equals(other.shippingAddress))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "{id=" + id + ", orderItemList=" + orderItemList + ", amount=" + amount + ", shippingAddress=" + shippingAddress + ", date=" + date + "}";
    }

}
