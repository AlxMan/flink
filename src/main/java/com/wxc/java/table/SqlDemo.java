package com.wxc.java.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class SqlDemo {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //tEnv
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
//                .inBatchMode()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database")
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        //读数据
        DataStreamSource<Tuple2<String, Integer>> data = env.addSource(new SourceFunction<Tuple2<String, Integer>>() {
            @Override
            public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
                int num = 0;
                while (true) {
                    num++;
                    ctx.collect(new Tuple2<>("name" + num, num));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });
        //将流式数据源转成table类型
        Table myTable = tEnv.fromDataStream(data, $("name"), $("num"));
        //查询,

        //table api:
        //Table selectResult = myTable.select($("name"), $("num")).filter($("num").mod(2).isEqual(0));

        //sql:
        tEnv.createTemporaryView("nameTable",data,$("name"),$("num"));
        Table selectResult = tEnv.sqlQuery("select * from nameTable where mod(num,2)=0");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(selectResult, Row.class);
        //保存处理结果
        tuple2DataStream.print();

        env.execute();
    }
}
