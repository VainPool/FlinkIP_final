package com.flink.ip.tables;

public class Nation {
    public Long n_nationkey;
    public String n_name;
    public Long n_regionkey;
    public String n_comment;

    public Nation() {
    }

    public Nation(Long nationKey, String name, Long regionKey, String comment) {
        n_nationkey = nationKey;
        n_name = name;
        n_regionkey = regionKey;
        n_comment = comment;
    }

    @Override
    public String toString() {
        return "Nation{" +
                "NationKey=" + n_nationkey +
                ", Name='" + n_name + '\'' +
                ", RegionKey=" + n_regionkey +
                ", Comment='" + n_comment + '\'' +
                '}';
    }
}
