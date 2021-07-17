package com.wxc.java.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class State {
    public static void main(String[] args) throws Exception {
        //（1,3）（1,5）（1,7）（1,4）（1,2）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        DataStreamSource<String> data = env.socketTextStream("linux121", 7777);
        SingleOutputStreamOperator<Tuple2<Long, Long>> maped = data.map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<Long, Long>(Long.valueOf(split[0]), Long.valueOf(split[1]));
            }
        });
//        DataStreamSource<Tuple2<Long,Long>> data = env.fromElements(new Tuple2(1l, 3l), new Tuple2(1l, 5l), new Tuple2(1l, 7l), new Tuple2(1l, 4l), new Tuple2(1l, 2l));

        KeyedStream<Tuple2<Long,Long>, Long> keyed = maped.keyBy(value -> value.f0);

        //按照key分组策略，对流式数据调用状态化处理
        SingleOutputStreamOperator<Tuple2<Long, Long>> flatMaped = keyed.flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            ValueState<Tuple2<Long, Long>> sumState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //在open方法中做出State
                ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                        "average",
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                        }),
                        Tuple2.of(0L, 0L)
                );

                sumState = getRuntimeContext().getState(descriptor);
                super.open(parameters);
            }

            @Override
            public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
                //在flatMap方法中，更新State
                Tuple2<Long, Long> currentSum = sumState.value();

                currentSum.f0 += 1;
                currentSum.f1 += value.f1;

                sumState.update(currentSum);

                if (currentSum.f0 == 2) {
                    long avarage = currentSum.f1 / currentSum.f0;
                    out.collect(new Tuple2<>(value.f0, avarage));
                    sumState.clear();
                }

            }
        });

        flatMaped.print();
        flatMaped.addSink(new OperaterState(2));

        env.execute();
    }
}