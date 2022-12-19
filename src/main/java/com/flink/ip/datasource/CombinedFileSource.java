package com.flink.ip.datasource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.util.Scanner;

public class CombinedFileSource implements ParallelSourceFunction<String> {
    private Boolean running = true;

    @Override
    public void run(SourceContext<String> allStream) throws Exception {
        Scanner sc = new Scanner(new File("combinedFile/combinedInput.tbl"));
        while(running){
            String i = sc.nextLine();
            allStream.collect(i);
            if (!sc.hasNext()){
                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
