package com.wxc.java.state;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 1、在Flink中，做出OperatorState有两种方式：1、实现CheckpointedFunction接口  2、实现ListCheckPointed
 * 2、两个方法：initializeState/snapshotState
 * initializeState:每一个Function在最开始的实例化的时候调用，方法内，实例化状态
 * snapshotState：每次checkpoint的时候被调用，将操作的最新数据放到最新的检查点中
 * 3、invoke：
 * 每来一个数据调用一次，把所有的到来的数据都放到缓存器中。目的是为了checkpoint的时候，从缓存器两种拿出数据
 *
 */
public class OperaterState implements SinkFunction<Tuple2<Long,Long>>, CheckpointedFunction {
    ListState<Tuple2<Long, Long>> operatorState;
    int threshold;

    private List<Tuple2<Long,Long>> bufferedElements;

    public OperaterState(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("....snapshotState");
        this.operatorState.clear();
        for (Tuple2<Long,Long> element : bufferedElements) {
            operatorState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("....initializeState");
        //做出一个State
        ListStateDescriptor<Tuple2<Long, Long>> operatarDemoDescriptor = new ListStateDescriptor<>(
                "operatarDemo",
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                })
        );
        operatorState = context.getOperatorStateStore().getListState(operatarDemoDescriptor);
        if(context.isRestored()) {//说明程序异常中断...nonono...just datasource was wrong,程序仍在努力容错
            for (Tuple2<Long,Long> element: operatorState.get()) {
                bufferedElements.add(element);
            }
            System.out.println("....context.isRestored():true" + bufferedElements);
        }

    }

    @Override
    public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
        System.out.println("---------invoke..........");
        bufferedElements.add(value);

        if(bufferedElements.size() == threshold) {
            //
            for(Tuple2<Long,Long> element : bufferedElements) {
                System.out.println("...out:" + element);
            }
            bufferedElements.clear();
        }
    }
}