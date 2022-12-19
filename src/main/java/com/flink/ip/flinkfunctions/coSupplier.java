package com.flink.ip.flinkfunctions;

import com.flink.ip.tables.Supplier;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class coSupplier extends CoProcessFunction <Tuple3<Long, Long,String>, Supplier, Tuple3<Long, Long, String>> {
    private ListState<Tuple3<Long, Long,String>> joinedTupleListState;
    private ListState<Supplier> supplierListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        joinedTupleListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Tuple3<Long, Long, String>>("t3_State", Types.TUPLE(Types.LONG, Types.LONG, Types.STRING))
        );

        supplierListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Supplier>("s_nationkeyState",Types.POJO(Supplier.class))
        );
    }

    @Override
    public void processElement1(Tuple3<Long, Long, String> joinedTuple, CoProcessFunction<Tuple3<Long, Long, String>, Supplier, Tuple3<Long, Long, String>>.Context context, Collector<Tuple3<Long, Long, String>> collector) throws Exception {
        if (supplierListState.get() != null){
            for (Supplier i : supplierListState.get()){
                collector.collect(new Tuple3(joinedTuple.f0, i.s_suppkey, joinedTuple.f2));
            }
        }
        joinedTupleListState.add(joinedTuple);
    }

    @Override
    public void processElement2(Supplier supplier, CoProcessFunction<Tuple3<Long, Long, String>, Supplier, Tuple3<Long, Long, String>>.Context context, Collector<Tuple3<Long, Long, String>> collector) throws Exception {
        if (joinedTupleListState.get() != null){
            for (Tuple3<Long, Long, String> i : joinedTupleListState.get()){
                collector.collect(new Tuple3(i.f0, supplier.s_suppkey, i.f2));
            }
        }
        supplierListState.add(supplier);
    }
}
