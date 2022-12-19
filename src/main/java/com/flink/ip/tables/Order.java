package com.flink.ip.tables;


import java.sql.Date;

public class Order {
    public Long o_orderkey;
    public Long o_custkey;
    public String o_orderstatus;
    public Double o_totalprice;
    public Date o_orderdate;
    public String o_orderpriority;
    public String o_clerk;
    public Integer o_shippriority;
    public String o_comment;

    public Order() {
    }

    public Order(Long orderKey, Long custKey, String orderStatus, Double totalPrice, String orderDate, String orderPriority, String clerk, Integer shipPriority, String comment) {
        o_orderkey = orderKey;
        o_custkey = custKey;
        o_orderstatus = orderStatus;
        o_totalprice = totalPrice;
        o_orderdate = Date.valueOf(orderDate);
        o_orderpriority = orderPriority;
        o_clerk = clerk;
        o_shippriority = shipPriority;
        o_comment = comment;
    }

    @Override
    public String toString() {
        return "Order{" +
                "OrderKey=" + o_orderkey +
                ", CustKey=" + o_custkey +
                ", OrderStatus='" + o_orderstatus + '\'' +
                ", TotalPrice=" + o_totalprice +
                ", OrderPriority='" + o_orderpriority + '\'' +
                ", OrderDate=" + o_orderdate +
                '}';
    }
}
