package com.flink.ip.flinkfunctionsforcheck;

import com.flink.ip.tablesforcheck.Nation;
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

public class coNation extends CoProcessFunction<Tuple3<Long, Long, Long>, Nation, Tuple4<Long, Long, Long, String>> {

    private ListState<Tuple3<Long, Long, Long>> joinedTupleListState;
    private ValueState<Nation> nationValueState;
    private ValueState<Boolean> cleared;

    @Override
    public void open(Configuration parameters) throws Exception {
        joinedTupleListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Tuple3<Long, Long, Long>>("t1_State", Types.TUPLE(Types.LONG, Types.LONG, Types.LONG))
        );

        nationValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Nation>("n_nationkeyState",Types.POJO(Nation.class))
        );

        cleared = getRuntimeContext().getState(
                new ValueStateDescriptor<Boolean>("cleared",Types.BOOLEAN)
        );
    }

    @Override
    public void processElement1(Tuple3<Long, Long, Long> joinedTuple, CoProcessFunction<Tuple3<Long, Long, Long>, Nation, Tuple4<Long, Long, Long, String>>.Context context, Collector<Tuple4<Long, Long, Long, String>> collector) throws Exception {
        // input tuple--Tuple3<o_custkey, o_orderkey, c_nationkey>
        // output tuple--Tuple4<o_orderkey, n_nationkey, n_regionkey, n_name>
        if (cleared.value() == null){
            cleared.update(false);
        }
        if (nationValueState.value() != null && !cleared.value()){
            joinedTupleListState.add(joinedTuple);
            for (Tuple3<Long, Long, Long> i : joinedTupleListState.get()){
                collector.collect(new Tuple4(i.f1, i.f2, nationValueState.value().n_regionkey, nationValueState.value().n_name));
            }
            joinedTupleListState.clear();
            cleared.update(true);
        }
        else if(nationValueState.value() != null && cleared.value()){
            collector.collect(new Tuple4(joinedTuple.f1, joinedTuple.f2, nationValueState.value().n_regionkey, nationValueState.value().n_name));
        }
        else if(nationValueState.value() == null){
            joinedTupleListState.add(joinedTuple);
        }
    }


    @Override
    public void processElement2(Nation nation, CoProcessFunction<Tuple3<Long, Long, Long>, Nation, Tuple4<Long, Long, Long, String>>.Context context, Collector<Tuple4<Long, Long, Long, String>> collector) throws Exception {
        nationValueState.update(nation);
        if (cleared.value() == null){
            cleared.update(false);
        }
        if (joinedTupleListState.get() != null){
            for (Tuple3<Long, Long, Long> i : joinedTupleListState.get()){
                collector.collect(new Tuple4(i.f1, i.f2, nationValueState.value().n_regionkey, nationValueState.value().n_name));
            }
            joinedTupleListState.clear();
            cleared.update(true);
        }
    }
}
