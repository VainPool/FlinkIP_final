package com.flink.ip.tablesforcheck;


import java.sql.Date;

public class Order {
    public Long o_orderkey;
    public Long o_custkey;
    public Date o_orderdate;
    public Integer perc;

    public Order() {
    }

    public Order(Long o_orderkey, Long o_custkey, String o_orderdate, Integer perc) {
        this.o_orderkey = o_orderkey;
        this.o_custkey = o_custkey;
        this.o_orderdate = Date.valueOf(o_orderdate);
        this.perc = perc;
    }

    @Override
    public String toString() {
        return "Order{" +
                "o_orderkey=" + o_orderkey +
                ", o_custkey=" + o_custkey +
                ", o_orderdate=" + o_orderdate +
                ", perc=" + perc +
                '}';
    }
}
