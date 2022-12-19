package com.flink.ip.tablesforcheck;

public class Nation {
    public Long n_nationkey;
    public Long n_regionkey;
    public String n_name;
    public Integer perc;

    public Nation() {
    }

    public Nation(Long n_nationkey, String n_name, Long n_regionkey, Integer perc) {
        this.n_nationkey = n_nationkey;
        this.n_regionkey = n_regionkey;
        this.n_name = n_name;
        this.perc = perc;
    }

    @Override
    public String toString() {
        return "Nation{" +
                "n_nationkey=" + n_nationkey +
                ", n_regionkey=" + n_regionkey +
                ", perc=" + perc +
                '}';
    }
}
