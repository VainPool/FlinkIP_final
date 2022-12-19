package com.flink.ip.tablesforcheck;


public class LineItem {
    public Long l_orderkey;
    public Long l_suppkey;
    public Double l_extendedprice;
    public Double l_discount;
    public Integer perc;

    public LineItem() {
    }

    public LineItem(Long l_orderkey, Long l_suppkey, Double l_extendedprice, Double l_discount, Integer perc) {
        this.l_orderkey = l_orderkey;
        this.l_suppkey = l_suppkey;
        this.l_extendedprice = l_extendedprice;
        this.l_discount = l_discount;
        this.perc = perc;
    }

    @Override
    public String toString() {
        return "LineItem{" +
                "l_orderkey=" + l_orderkey +
                ", l_suppkey=" + l_suppkey +
                ", l_extendedprice=" + l_extendedprice +
                ", l_discount=" + l_discount +
                ", perc=" + perc +
                '}';
    }
}
