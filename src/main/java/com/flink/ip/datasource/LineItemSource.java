package com.flink.ip.datasource;

import com.flink.ip.tables.LineItem;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.util.Scanner;

public class LineItemSource implements ParallelSourceFunction<LineItem> {
    private Boolean running = true;
    public String dataPath;

    public LineItemSource() {
        dataPath = "input/";
    }

    public LineItemSource(String dataPath) {
        this.dataPath = dataPath;
    }

    @Override
    public void run(SourceContext<LineItem> lintItemStream) throws Exception {
        Scanner sc = new Scanner(new File(dataPath+"lineitem.tbl"));
        while (running) {
            String[] lineitem = sc.nextLine().split("\\|");
            LineItem added = new LineItem(Long.valueOf(lineitem[0]),
                    Long.valueOf(lineitem[1]),
                    Long.valueOf(lineitem[2]),
                    Integer.valueOf(lineitem[3]),
                    Double.valueOf(lineitem[4]),
                    Double.valueOf(lineitem[5]),
                    Double.valueOf(lineitem[6]),
                    Double.valueOf(lineitem[7]),
                    lineitem[8],
                    lineitem[9],
                    lineitem[10],
                    lineitem[11],
                    lineitem[12],
                    lineitem[13],
                    lineitem[14],
                    lineitem[15]
            );
            lintItemStream.collect(added);
            //Thread.sleep(1000);
            if (!sc.hasNext()) {
                System.out.println("lineitem finished:"+System.currentTimeMillis());
                running = false;
            }
        }

    }

    @Override
    public void cancel() {
        running = false;

    }
}
