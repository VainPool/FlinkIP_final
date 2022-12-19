package com.flink.ip.tables;

import java.sql.Date;

public class LineItem {
    public Long l_orderkey;
    public Long l_partkey;
    public Long l_suppkey;
    public Integer l_linenumber;
    public Double l_quantity;
    public Double l_extendedprice;
    public Double l_discount;
    public Double l_tax;
    public String l_returnflag;
    public String l_linestatus;
    public Date l_shipdate;
    public Date l_commitdate;
    public Date l_receiptdate;
    public String l_shipinstruct;
    public String l_shipmode;
    public String l_comment;

    public LineItem() {
    }

    public LineItem(Long orderKey, Long partKey, Long suppKey, Integer lineNumber, Double quantity, Double extendedPrice, Double discount, Double tax, String returnFlag, String lineStatus, String shipDate, String commitDate, String receiptDate, String shipInStruct, String shipMode, String comment) {
        l_orderkey = orderKey;
        l_partkey = partKey;
        l_suppkey = suppKey;
        l_linenumber = lineNumber;
        l_quantity = quantity;
        l_extendedprice = extendedPrice;
        l_discount = discount;
        l_tax = tax;
        l_returnflag = returnFlag;
        l_linestatus = lineStatus;
        l_shipdate = Date.valueOf(shipDate);
        l_commitdate = Date.valueOf(commitDate);
        l_receiptdate = Date.valueOf(receiptDate);
        l_shipinstruct = shipInStruct;
        l_shipmode = shipMode;
        l_comment = comment;
    }

    @Override
    public String toString() {
        return "LineItem{" +
                "OrderKey=" + l_orderkey +
                ", PartKey=" + l_partkey +
                ", SuppKey=" + l_suppkey +
                '}';
    }
}
