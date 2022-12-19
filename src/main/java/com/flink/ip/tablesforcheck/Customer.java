package com.flink.ip.tablesforcheck;

public class Customer {
    public Long c_custkey;
    public Long c_nationkey;
    public Integer perc;

    public Customer(){
    }

    public Customer(Long c_custkey, Long c_nationkey, Integer perc) {
        this.c_custkey = c_custkey;
        this.c_nationkey = c_nationkey;
        this.perc = perc;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "c_custkey=" + c_custkey +
                ", c_nationkey=" + c_nationkey +
                ", perc=" + perc +
                '}';
    }
}
