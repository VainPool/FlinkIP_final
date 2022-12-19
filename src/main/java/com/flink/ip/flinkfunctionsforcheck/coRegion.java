package com.flink.ip.flinkfunctionsforcheck;

import com.flink.ip.tablesforcheck.Region;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class coRegion extends CoProcessFunction <Tuple4<Long, Long, Long, String>, Region, Tuple3<Long, Long, String>>{
    private ListState<Tuple4<Long, Long, Long, String>> joinedTupleListState;
    private ValueState<Region> regionValueState;
    private ValueState<Boolean> cleared;

    @Override
    public void open(Configuration parameters) throws Exception {
        joinedTupleListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Tuple4<Long, Long, Long, String>>("t2_State", Types.TUPLE(Types.LONG, Types.LONG, Types.LONG, Types.STRING))
        );

        regionValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Region>("r_regionkeyState",Types.POJO(Region.class))
        );

        cleared = getRuntimeContext().getState(
                new ValueStateDescriptor<Boolean>("cleared",Types.BOOLEAN)
        );
    }

    @Override
    public void processElement1(Tuple4<Long, Long, Long, String> joinedTuple, CoProcessFunction<Tuple4<Long, Long, Long, String>, Region, Tuple3<Long, Long, String>>.Context context, Collector<Tuple3<Long, Long, String>> collector) throws Exception {
        // input tuple--Tuple4<o_orderkey, n_nationkey, n_regionkey, n_name>
        // output tuple--Tuple3<o_orderkey, r_nationkey, n_name>
        if (cleared.value() == null){
            cleared.update(false);
        }
        if (regionValueState.value() != null && !cleared.value()){
            joinedTupleListState.add(joinedTuple);
            for (Tuple4<Long, Long, Long, String> i : joinedTupleListState.get()){
                collector.collect(new Tuple3(i.f0, i.f1, i.f3));
            }
            joinedTupleListState.clear();
            cleared.update(true);
        }
        else if(regionValueState.value() != null && cleared.value()){
            collector.collect(new Tuple3(joinedTuple.f0, joinedTuple.f1, joinedTuple.f3));
        }
        else if(regionValueState.value() == null){
            joinedTupleListState.add(joinedTuple);
        }

    }

    @Override
    public void processElement2(Region region, CoProcessFunction<Tuple4<Long, Long, Long, String>, Region, Tuple3<Long, Long, String>>.Context context, Collector<Tuple3<Long, Long, String>> collector) throws Exception {
        regionValueState.update(region);
        if (cleared.value() == null){
            cleared.update(false);
        }
        if (joinedTupleListState.get() != null){
            for (Tuple4<Long, Long, Long, String> i : joinedTupleListState.get()){
                collector.collect(new Tuple3(i.f0, i.f1, i.f3));
            }
            joinedTupleListState.clear();
            cleared.update(true);
        }
    }
}
