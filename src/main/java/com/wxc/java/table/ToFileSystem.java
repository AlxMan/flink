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
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class ToFileSystem {
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

        Table nameTable = tEnv.fromDataStream(data, $("name"), $("num"));

        //将数据保存到文件系统中
        tEnv.connect(new FileSystem().path("data/output"))
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                                .field("name", DataTypes.STRING())
                        .field("num",DataTypes.INT())
                )
                .createTemporaryTable("tmpTable");

        nameTable.executeInsert("tmpTable");


    }
}
