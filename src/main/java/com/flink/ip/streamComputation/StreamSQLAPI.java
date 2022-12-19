package com.flink.ip.streamComputation;

import com.flink.ip.datasource.*;
import com.flink.ip.tables.Customer;
import com.flink.ip.tables.Nation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Scanner;

public class StreamSQLAPI {
    public static void main(String[] args) throws Exception{
        String date = "1993-12-31";
        String selectedRegion = "AFRICA";
        String dataPath = "input/";

        StreamExecutionEnvironment testenv = StreamExecutionEnvironment.getExecutionEnvironment();

        //testenv.setParallelism(1);

        if(args.length>0){
            for (String arg: args){
                dataPath = arg;
            }
        }

        DataStreamSource<Customer> customerSc = testenv.addSource(new CustomerSource(dataPath)).setParallelism(1);
        DataStreamSource<Nation> nationSc = testenv.addSource(new NationSource(dataPath)).setParallelism(1);
        DataStreamSource lineitemSc = testenv.addSource(new LineItemSource(dataPath)).setParallelism(1);
        DataStreamSource orderSc = testenv.addSource(new OrdersSource(dataPath)).setParallelism(1);
        DataStreamSource regionSc = testenv.addSource(new RegionSource(dataPath)).setParallelism(1);
        DataStreamSource supplierSc = testenv.addSource(new SupplierSource(dataPath)).setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(testenv);

        Table custTable = tableEnv.fromDataStream(customerSc);
        Table nationTable = tableEnv.fromDataStream(nationSc);
        Table lineItemTable = tableEnv.fromDataStream(lineitemSc);
        Table orderTable = tableEnv.fromDataStream(orderSc);
        Table regionTable = tableEnv.fromDataStream(regionSc);
        Table supplierTable = tableEnv.fromDataStream(supplierSc);

        tableEnv.createTemporaryView("custView",custTable);
        tableEnv.createTemporaryView("nationView",nationTable);
        tableEnv.createTemporaryView("lineItemView",lineItemTable);
        tableEnv.createTemporaryView("orderView",orderTable);
        tableEnv.createTemporaryView("regionView",regionTable);
        tableEnv.createTemporaryView("supplierView",supplierTable);




        Table resultTable = tableEnv.sqlQuery("select " +
                "n_name, " +
                "sum(l_extendedprice * (1 - l_discount)) as revenue, " +
                "count(1) " +
                "from " +
                "custView, " +
                "orderView, " +
                "lineItemView, " +
                "supplierView, " +
                "nationView, " +
                "regionView " +
                "where " +
                "c_custkey = o_custkey " +
                "and l_orderkey = o_orderkey " +
                "and l_suppkey = s_suppkey " +
                "and c_nationkey = s_nationkey " +
                "and s_nationkey = n_nationkey " +
                "and n_regionkey = r_regionkey " +
                "and r_name = '"+selectedRegion+"' " +
                "and o_orderdate >= date '"+date+"' " +
                "and o_orderdate < date '"+date+"' + interval '1' year " +
                "group by " +
                "n_name");

        tableEnv.createTemporaryView("resultView",resultTable);

        Table sortedResult = tableEnv.sqlQuery("select * from (select n_name, revenue, ROW_NUMBER() over (ORDER BY revenue desc) as rownum from resultView) WHERE rownum <= 10000000");

        tableEnv.toChangelogStream(sortedResult).print();

        testenv.execute();
    }
}
