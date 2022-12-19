package com.flink.ip.tablesforcheck;

public class Supplier {
    public Long s_suppkey;
    public Long s_nationkey;
    public Integer perc;

    public Supplier() {
    }

    public Supplier(Long s_suppkey, Long s_nationkey, Integer perc) {
        this.s_suppkey = s_suppkey;
        this.s_nationkey = s_nationkey;
        this.perc = perc;
    }

    @Override
    public String toString() {
        return "Supplier{" +
                "s_suppkey=" + s_suppkey +
                ", s_nationkey=" + s_nationkey +
                ", perc=" + perc +
                '}';
    }
}
