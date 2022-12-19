package com.flink.ip.datasource;

import com.flink.ip.tables.Customer;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.util.Scanner;

public class CustomerSource implements ParallelSourceFunction<Customer> {
    public Boolean running = true;
    public Long customerEndTime;
    public String dataPath;

    public CustomerSource() {
        this.dataPath = "input/";
    }

    public CustomerSource(String dataPath) {
        this.dataPath = dataPath;
    }

    @Override
    public void run(SourceContext<Customer> custStream) throws Exception {
        Scanner sc = new Scanner(new File(dataPath+"customer.tbl"));
        while(running){
            String [] cust = sc.nextLine().split("\\|");
            Customer added = new Customer(Long.valueOf(cust[0]),
                    cust[1],
                    cust[2],
                    Long.valueOf(cust[3]),
                    cust[4],
                    Double.valueOf(cust[5]),
                    cust[6],
                    cust[7]);
            custStream.collect(added);
            //Thread.sleep(1000);
            if (!sc.hasNext()){
                customerEndTime = System.currentTimeMillis();
                System.out.println("customer finished:"+customerEndTime);
                running = false;
/*                System.out.println("cs running false time:"+System.currentTimeMillis());
                System.out.println(running);*/
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
