package com.flink.ip.datasource;

import com.flink.ip.tables.Region;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.util.Scanner;

public class RegionSource implements ParallelSourceFunction<Region> {
    private Boolean running = true;
    public String dataPath;

    public RegionSource() {
        dataPath = "input/";
    }

    public RegionSource(String dataPath) {
        this.dataPath = dataPath;
    }

    @Override
    public void run(SourceContext<Region> regionStream) throws Exception {
        Scanner sc = new Scanner(new File(dataPath+"region.tbl"));
        while (running) {
            String[] region = sc.nextLine().split("\\|");
            Region added = new Region(Long.valueOf(region[0]),
                    region[1],
                    region[2]
            );
            regionStream.collect(added);
            //Thread.sleep(1000);
            if (!sc.hasNext()) {
                System.out.println("region finished:"+System.currentTimeMillis());
                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;

    }
}
