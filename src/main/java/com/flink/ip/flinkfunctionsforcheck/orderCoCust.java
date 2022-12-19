package com.flink.ip.flinkfunctionsforcheck;

import com.flink.ip.tablesforcheck.Customer;
import com.flink.ip.tablesforcheck.Order;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class orderCoCust extends CoProcessFunction<Order, Customer, Tuple3<Long, Long, Long>> {

    private ListState<Order> orderListState;
    private ValueState<Customer> customerValueState;
    private ValueState<Boolean> cleared;

    @Override
    public void open(Configuration parameters) throws Exception {
        orderListState = getRuntimeContext().getListState(
                new ListStateDescriptor<Order>("o_custkeyState", Types.POJO(Order.class))
        );

        customerValueState = getRuntimeContext().getState(
                new ValueStateDescriptor<Customer>("c_custkeyState",Types.POJO(Customer.class))
        );

        cleared = getRuntimeContext().getState(
                new ValueStateDescriptor<Boolean>("cleared",Types.BOOLEAN)
        );
    }

    @Override
    public void processElement1(Order order, CoProcessFunction<Order, Customer, Tuple3<Long, Long, Long>>.Context context, Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
        // output tuple--Tuple3<o_custkey, o_orderkey, c_nationkey>
        if (cleared.value() == null){
            cleared.update(false);
        }
        if (customerValueState.value() != null && !cleared.value()){
            orderListState.add(order);
            for (Order i : orderListState.get()){
                collector.collect(new Tuple3(i.o_custkey, i.o_orderkey, customerValueState.value().c_nationkey));
            }
            orderListState.clear();
            cleared.update(true);
        }
        else if(customerValueState.value() != null && cleared.value()){
            collector.collect(new Tuple3(order.o_custkey, order.o_orderkey, customerValueState.value().c_nationkey));
        }
        else if(customerValueState.value() == null){
            orderListState.add(order);
        }
    }

    @Override
    public void processElement2(Customer customer, CoProcessFunction<Order, Customer, Tuple3<Long, Long, Long>>.Context context, Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
        customerValueState.update(customer);
        if (cleared.value() == null){
            cleared.update(false);
        }
        if (orderListState.get() != null){
            for (Order i : orderListState.get()){
                collector.collect(new Tuple3(i.o_custkey, i.o_orderkey, customerValueState.value().c_nationkey));
            }
            orderListState.clear();
            cleared.update(true);
        }
    }
}
