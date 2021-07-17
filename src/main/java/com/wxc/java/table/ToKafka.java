package com.wxc.java.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class ToKafka {
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
        DataStreamSource<String> data = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int num = 0;
                while (true) {
                    num++;
                    ctx.collect("name" + num);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        Table nameTable = tEnv.fromDataStream(data, $("name"));

        tEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic("animal")
                        .startFromEarliest()
                        .property("bootstrap.servers","node52:9092")
        )
                .withFormat(new Csv())
                .withSchema(
                        new Schema().field("name", DataTypes.STRING())
                )
                .createTemporaryTable("animalTable");

        nameTable.executeInsert("animalTable");

        env.execute();
    }
}
