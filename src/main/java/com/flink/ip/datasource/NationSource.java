package com.flink.ip.datasource;


import com.flink.ip.tables.Nation;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.util.Scanner;

public class NationSource implements ParallelSourceFunction<Nation> {
    private Boolean running = true;
    public String dataPath;

    public NationSource() {
        dataPath = "input/";
    }

    public NationSource(String dataPath) {
        this.dataPath = dataPath;
    }

    @Override
    public void run(SourceContext<Nation> nationStream) throws Exception {
        Scanner sc = new Scanner(new File(dataPath+"nation.tbl"));
        while (running) {
            String[] nation = sc.nextLine().split("\\|");
            Nation added = new Nation(Long.valueOf(nation[0]),
                    nation[1],
                    Long.valueOf(nation[2]),
                    nation[3]
            );
            nationStream.collect(added);
            //Thread.sleep(1000);
            if (!sc.hasNext()) {
                System.out.println("nation finished:"+System.currentTimeMillis());
                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;

    }
}
