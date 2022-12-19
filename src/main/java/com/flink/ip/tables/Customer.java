package com.flink.ip.tables;

public class Customer {
    public Long c_custkey;
    public String c_name;
    public String c_address;
    public Long c_nationkey;
    public String c_phone;
    public Double c_acctbal;
    public String c_mktsegment;
    public String c_comment;

    public Customer(){
    }

    public Customer(Long CustKey, String Name, String Address, Long NationKey, String Phone, Double AcctBal, String MktSegment, String Comment){
        this.c_custkey = CustKey;
        this.c_name = Name;
        this.c_address = Address;
        this.c_nationkey = NationKey;
        this.c_phone = Phone;
        this.c_acctbal = AcctBal;
        this.c_mktsegment = MktSegment;
        this.c_comment = Comment;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "CustKey=" + c_custkey +
                ", Name='" + c_name + '\'' +
                ", AcctBal=" + c_acctbal +
                ", MktSegment='" + c_mktsegment + '\'' +
                '}';
    }

}
