package com.wxc.java.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("linux121", 7777);
        SingleOutputStreamOperator<String> mapped = data.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        KeyedStream<String, String> keyByed = mapped.keyBy(value -> value);
        WindowedStream<String, String, TimeWindow> sesionWindow = keyByed.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
        SingleOutputStreamOperator<String> applyed = sesionWindow.apply(new WindowFunction<String, String, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<String> input, Collector<String> out) throws Exception {
                StringBuilder sb = new StringBuilder();
                for (String str : input) {
                    sb.append(str);
                }
                out.collect(sb.toString());
            }
        });

        applyed.print();
        env.execute();
    }
}
