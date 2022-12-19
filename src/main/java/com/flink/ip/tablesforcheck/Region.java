package com.flink.ip.tablesforcheck;

public class Region {
    public Long r_regionkey;
    public String r_name;
    public Integer perc;

    public Region() {
    }

    public Region(Long regionKey, String name, Integer perc) {
        r_regionkey = regionKey;
        r_name = name;
        this.perc = perc;
    }

    @Override
    public String toString() {
        return "Region{" +
                "r_regionkey=" + r_regionkey +
                ", r_name='" + r_name + '\'' +
                ", perc=" + perc +
                '}';
    }
}
