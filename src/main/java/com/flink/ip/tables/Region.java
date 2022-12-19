package com.flink.ip.tables;

public class Region {
    public Long r_regionkey;
    public String r_name;
    public String r_comment;

    public Region() {
    }

    public Region(Long regionKey, String name, String comment) {
        r_regionkey = regionKey;
        r_name = name;
        r_comment = comment;
    }

    @Override
    public String toString() {
        return "Region{" +
                "RegionKey=" + r_regionkey +
                ", Name='" + r_name + '\'' +
                ", Comment='" + r_comment + '\'' +
                '}';
    }
}
