package com.flink.ip.datasource;

import com.flink.ip.tables.Supplier;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.util.Scanner;

public class SupplierSource implements ParallelSourceFunction<Supplier> {
    private Boolean running = true;
    public String dataPath;

    public SupplierSource() {
        dataPath = "input/";
    }

    public SupplierSource(String dataPath) {
        this.dataPath = dataPath;
    }

    @Override
    public void run(SourceContext<Supplier> supplierStream) throws Exception {
        Scanner sc = new Scanner(new File(dataPath+"supplier.tbl"));
        while (running) {
            String[] supplier = sc.nextLine().split("\\|");
            Supplier added = new Supplier(Long.valueOf(supplier[0]),
                    supplier[1],
                    supplier[2],
                    Long.valueOf(supplier[3]),
                    supplier[4],
                    Double.valueOf(supplier[5]),
                    supplier[6]
            );
            supplierStream.collect(added);
            //Thread.sleep(1000);
            if (!sc.hasNext()) {
                System.out.println("supplier finished:"+System.currentTimeMillis());
                running = false;
            }
        }

    }

    @Override
    public void cancel() {
        running = false;

    }
}
