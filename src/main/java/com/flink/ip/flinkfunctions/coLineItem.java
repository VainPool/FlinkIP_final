package com.flink.ip.flinkfunctions;

import com.flink.ip.tables.LineItem;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class coLineItem extends CoProcessFunction<Tuple3<Long, Long, String>, LineItem, Tuple2<String, Double>>{
    //private ListState<Tuple3<Long, Long, String>> joinedTupleListState;
    private ValueState<Tuple3<Long, Long, String>> joinedTupleValueState;
    private ListState<LineItem> lineItemListState;
    private ValueState<Boolean> cleared;

    @Override
    public void open(Configuration parameters) throws Exception {
/*        joinedTupleListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Tuple3<Long, Long, String>>("t4_State", Types.TUPLE(Types.LONG, Types.LONG, Types.STRING))
        );*/

        joinedTupleValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Tuple3<Long, Long, String>>("t4_State", Types.TUPLE(Types.LONG, Types.LONG, Types.STRING))
        );

        lineItemListState = getRuntimeContext().getListState(
                new ListStateDescriptor<LineItem>("l_combinekeyState",Types.POJO(LineItem.class))
        );

        cleared = getRuntimeContext().getState(
                new ValueStateDescriptor<Boolean>("cleared",Types.BOOLEAN)
        );
    }

    @Override
    public void processElement1(Tuple3<Long, Long, String> joinedTuple, CoProcessFunction<Tuple3<Long, Long, String>, LineItem, Tuple2<String, Double>>.Context context, Collector<Tuple2<String, Double>> collector) throws Exception {
        // input tuple--Tuple3<o_orderkey, s_suppkey, r_name>
        // output tuple--Tuple2<n_name, l_extendedprice * (1 - l_discount)>

/*        if (lineItemListState.get() != null){
            for (LineItem i : lineItemListState.get()){
                collector.collect(new Tuple2(joinedTuple.f2, i.l_extendedprice*(1.00-i.l_discount)));
            }
        }
        joinedTupleListState.add(joinedTuple);*/

        joinedTupleValueState.update(joinedTuple);
        if (cleared.value() == null){
            cleared.update(false);
        }
        if (lineItemListState.get() != null){
            for (LineItem i : lineItemListState.get()){
                collector.collect(new Tuple2(joinedTuple.f2, i.l_extendedprice*(1.00-i.l_discount)));
            }
            lineItemListState.clear();
            cleared.update(true);
        }

    }

    @Override
    public void processElement2(LineItem lineItem, CoProcessFunction<Tuple3<Long, Long, String>, LineItem, Tuple2<String, Double>>.Context context, Collector<Tuple2<String, Double>> collector) throws Exception {

/*        if (joinedTupleListState.get() != null){
            for (Tuple3<Long, Long, String> i : joinedTupleListState.get()){
                collector.collect(new Tuple2(i.f2, lineItem.l_extendedprice*(1.00-lineItem.l_discount)));
            }
        }
        lineItemListState.add(lineItem);*/
        if (cleared.value() == null){
            cleared.update(false);
        }
        if (joinedTupleValueState.value() != null && !cleared.value()){
            lineItemListState.add(lineItem);
            for (LineItem i : lineItemListState.get()){
                collector.collect(new Tuple2(joinedTupleValueState.value().f2, lineItem.l_extendedprice*(1.00-lineItem.l_discount)));
            }
            lineItemListState.clear();
            cleared.update(true);
        }
        else if(joinedTupleValueState.value() != null && cleared.value()){
            collector.collect(new Tuple2(joinedTupleValueState.value().f2, lineItem.l_extendedprice*(1.00-lineItem.l_discount)));
        }
        else if(joinedTupleValueState.value() == null){
            lineItemListState.add(lineItem);
        }
    }
}
