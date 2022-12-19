package com.flink.ip.datasource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.util.Scanner;

public class CombinedFileSourceForCheck implements ParallelSourceFunction<String> {
    private Boolean running = true;
    private String stoptag;

    public CombinedFileSourceForCheck() {
    }

    public CombinedFileSourceForCheck(String stoptag) {
        this.stoptag = stoptag;
    }

    @Override
    public void run(SourceContext<String> allStream) throws Exception {
        Scanner sc = new Scanner(new File("combinedFile/combinedInput.tbl"));
        while(running){
            String i = sc.nextLine();
            allStream.collect(i);

            if (!sc.hasNext() || i.equals(stoptag)){
                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
