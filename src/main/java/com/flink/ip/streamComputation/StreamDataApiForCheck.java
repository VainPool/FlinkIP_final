package com.flink.ip.streamComputation;

import com.flink.ip.datasource.CombinedFileSourceForCheck;
import com.flink.ip.flinkfunctionsforcheck.*;
import com.flink.ip.tablesforcheck.*;
import com.flink.ip.util.MySQLProcessing;
import com.flink.ip.util.Preprocessing;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Date;
import java.util.Calendar;
import java.util.Scanner;

public class StreamDataApiForCheck {
    public static void main(String[] args) throws Exception{
        String mysqlUrl = "jdbc:mysql://localhost:3306/ip2";
        String mysqlDriverName = "com.mysql.cj.jdbc.Driver";
        String mysqlUserName = "root";
        String mysqlPassword = "root";
        MySQLProcessing sqlprocess = new MySQLProcessing(mysqlUrl,mysqlDriverName,mysqlUserName,mysqlPassword);
        String mysqlSafePath = "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/";

        Date inputDate = Date.valueOf("1995-09-17");
        Calendar cal = Calendar.getInstance();
        cal.setTime(inputDate);
        cal.add(Calendar.YEAR,1);
        Date date = new Date(cal.getTimeInMillis());
        String selectedRegion = "ASIA";
        
        System.out.println("Please input percentage of data to process(25%, 50%, 75% or 100%, default is 100%):");
        Scanner scanner1 = new Scanner(System.in);
        String perc = scanner1.nextLine();
        perc = perc.isEmpty()?"100%":perc;

        System.out.println("Do preprocessing?(Y/N, default is N)");
        String preprocess = scanner1.nextLine();
        preprocess = preprocess.isEmpty()?"N":preprocess;
        scanner1.close();

        System.out.println("Processing data percentage is:"+perc);
        System.out.println("Do preprocessing:"+preprocess);
        System.out.println("Running now, please wait...");

        if (preprocess.equals("Y")){
            sqlprocess.executeDDL();
            sqlprocess.executeDelete();
            Preprocessing process = new Preprocessing("input/", mysqlSafePath, mysqlUrl, mysqlDriverName, mysqlUserName, mysqlPassword);
            System.out.println(process.execute());
        }

        StreamExecutionEnvironment testenv = StreamExecutionEnvironment.getExecutionEnvironment();

        OutputTag<Nation> nationTag = new OutputTag<Nation>("nation"){};
        OutputTag<Customer> customerTag = new OutputTag<Customer>("customer"){};
        OutputTag<LineItem> lineItemTag = new OutputTag<LineItem>("lineItem"){};
        OutputTag<Order> orderTag = new OutputTag<Order>("order"){};
        OutputTag<Region> regionTag = new OutputTag<Region>("region"){};
        OutputTag<Supplier> supplierTag = new OutputTag<Supplier>("supplier"){};

        SingleOutputStreamOperator<String> allSc = testenv.addSource(new CombinedFileSourceForCheck(perc)).setParallelism(1)
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, String>.Context ctx, Collector<String> output) throws Exception {
                        String[] ct = s.split("\\|");
                        if (ct[0].equals("nation.tbl")) {
                            ctx.output(nationTag, new Nation(Long.valueOf(ct[1]), ct[2], Long.valueOf(ct[3]), Integer.valueOf(ct[5])));
                        }
                        else if(ct[0].equals("customer.tbl")){
                            ctx.output(customerTag, new Customer(Long.valueOf(ct[1]),
                                    Long.valueOf(ct[4]),
                                    Integer.valueOf(ct[9])));
                        }
                        else if(ct[0].equals("lineitem.tbl")){
                            ctx.output(lineItemTag, new LineItem(Long.valueOf(ct[1]),
                                    Long.valueOf(ct[3]),
                                    Double.valueOf(ct[6]),
                                    Double.valueOf(ct[7]),
                                    Integer.valueOf(ct[17])));
                        }
                        else if(ct[0].equals("orders.tbl")){
                            ctx.output(orderTag, new Order(Long.valueOf(ct[1]),
                                    Long.valueOf(ct[2]),
                                    ct[5],
                                    Integer.valueOf(ct[10])));
                        }
                        else if(ct[0].equals("region.tbl")){
                            ctx.output(regionTag, new Region(Long.valueOf(ct[1]),
                                    ct[2],
                                    Integer.valueOf(ct[4])));
                        }
                        else if(ct[0].equals("supplier.tbl")){
                            ctx.output(supplierTag, new Supplier(Long.valueOf(ct[1]),
                                    Long.valueOf(ct[4]),
                                    Integer.valueOf(ct[8])));
                        }
                        else {
                            output.collect(s);
                        }
                    }
                });

        DataStream<Customer> customerSc = allSc.getSideOutput(customerTag);
        DataStream<Nation> nationSc = allSc.getSideOutput(nationTag);
        DataStream<LineItem> lineitemSc = allSc.getSideOutput(lineItemTag);
        DataStream<Order> orderSc = allSc.getSideOutput(orderTag);
        DataStream<Region> regionSc = allSc.getSideOutput(regionTag);
        DataStream<Supplier> supplierSc = allSc.getSideOutput(supplierTag);


        orderSc.filter(order -> !order.o_orderdate.before(inputDate) && order.o_orderdate.before(date))
                .connect(customerSc)
                .keyBy("o_custkey", "c_custkey")
                .process(new orderCoCust())         //Tuple3<o_custkey, o_orderkey, c_nationkey>
                .connect(nationSc)
                .keyBy(data -> data.f2, data -> data.n_nationkey)
                .process(new coNation())        //Tuple4<o_orderkey, n_nationkey, n_regionkey, n_name>
                .connect(regionSc.filter(region -> region.r_name.equals(selectedRegion)))
                .keyBy(data -> data.f2, data -> data.r_regionkey)
                .process(new coRegion())        //Tuple3<o_orderkey, r_nationkey, n_name>
                .connect(supplierSc)
                .keyBy(data -> data.f1, data -> data.s_nationkey)
                .process(new coSupplier())      //Tuple3<o_orderkey, s_suppkey, r_name>
                .connect(lineitemSc)
                .keyBy(data -> data.f0.toString() + "#" + data.f1.toString(), data -> data.l_orderkey.toString() + "#" + data.l_suppkey.toString())
                .process(new coLineItem())      //Tuple3<r_name, sum(l_extendedprice * (1 - l_discount))>
                .keyBy(data -> "key")
                .process(new sumAndOrder())
                .print();

        testenv.execute();
    }
}
