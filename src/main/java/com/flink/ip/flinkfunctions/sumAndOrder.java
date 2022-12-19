package com.flink.ip.flinkfunctions;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class sumAndOrder extends KeyedProcessFunction<String, Tuple2<String, Double>, ArrayList<Tuple2<String,Double>>> {
    private MapState<String, Double> addMapState;
    private ArrayList<Tuple2<String,Double>> result = new ArrayList<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        addMapState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("mapState", Types.STRING, Types.DOUBLE)
        );
    }

    @Override
    public void processElement(Tuple2<String, Double> inputTuple, KeyedProcessFunction<String, Tuple2<String, Double>, ArrayList<Tuple2<String, Double>>>.Context context, Collector<ArrayList<Tuple2<String, Double>>> collector) throws Exception {
        if(!addMapState.contains(inputTuple.f0)){
            addMapState.put(inputTuple.f0, inputTuple.f1);
        }
        else{
            Double i = addMapState.get(inputTuple.f0);
            addMapState.put(inputTuple.f0, i+inputTuple.f1);
        }

        ArrayList<Tuple2<String,Double>> result = new ArrayList<>();
        for (String k : addMapState.keys()){
            result.add(new Tuple2<String, Double>(k, addMapState.get(k)));
        }
        result.sort(new Comparator<Tuple2<String, Double>>() {
            @Override
            public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                if (o2.f1 - o1.f1 > 0){
                    return 1;
                }
                else{
                    return -1;
                }
            }
        });
        this.result = result;
        collector.collect(result);
    }


    @Override
    public void close() throws Exception {
/*        ArrayList<Tuple2<String,Double>> result = new ArrayList<>();
        for (String k : addMapState.keys()){
            result.add(new Tuple2<String, Double>(k, addMapState.get(k)));
        }
        result.sort(new Comparator<Tuple2<String, Double>>() {
            @Override
            public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                if (o2.f1 - o1.f1 > 0){
                    return 1;
                }
                else{
                    return -1;
                }
            }
        });*/
        //System.out.println("final result is:"+result);
    }
}
