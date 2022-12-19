package com.flink.ip.tables;

public class Supplier {
    public Long s_suppkey;
    public String s_name;
    public String s_address;
    public Long s_nationkey;
    public String s_phone;
    public Double s_acctbal;
    public String s_comment;

    public Supplier() {
    }

    public Supplier(Long suppKey, String name, String address, Long nationKey, String phone, Double acctBal, String comment) {
        s_suppkey = suppKey;
        s_name = name;
        s_address = address;
        s_nationkey = nationKey;
        s_phone = phone;
        s_acctbal = acctBal;
        s_comment = comment;
    }

    @Override
    public String toString() {
        return "Supplier{" +
                "SuppKey=" + s_suppkey +
                ", Name='" + s_name + '\'' +
                ", Address='" + s_address + '\'' +
                ", NationKey=" + s_nationkey +
                '}';
    }
}
