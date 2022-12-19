package com.flink.ip.datasource;

import com.flink.ip.tables.Order;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.util.Scanner;

public class OrdersSource implements ParallelSourceFunction<Order> {
    private Boolean running = true;
    public String dataPath;

    public OrdersSource() {
        dataPath = "input/";
    }

    public OrdersSource(String dataPath) {
        this.dataPath = dataPath;
    }

    @Override
    public void run(SourceContext<Order> orderStream) throws Exception {
        Scanner sc = new Scanner(new File(dataPath+"orders.tbl"));
        while (running) {
            String[] order = sc.nextLine().split("\\|");
            Order added = new Order(Long.valueOf(order[0]),
                    Long.valueOf(order[1]),
                    order[2],
                    Double.valueOf(order[3]),
                    order[4],
                    order[5],
                    order[6],
                    Integer.valueOf(order[7]),
                    order[8]
            );
            orderStream.collect(added);
            //Thread.sleep(100);
            if (!sc.hasNext()) {
                System.out.println("order finished:"+System.currentTimeMillis());
                running = false;
            }
        }

    }

    @Override
    public void cancel() {
        running = false;

    }
}
