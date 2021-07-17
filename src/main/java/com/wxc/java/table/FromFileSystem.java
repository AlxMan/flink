package com.wxc.java.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

public class FromFileSystem {
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

        //从文件系统中获取数据
        tEnv.connect(new FileSystem().path("data/input/hello.txt"))
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                        .field("name", DataTypes.STRING())
                )
                .createTemporaryTable("nameTable");

        String sql = "select * from nameTable";
        Table resultTable = tEnv.sqlQuery(sql);

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(resultTable, Row.class);

        tuple2DataStream.print();

        env.execute();


    }
}
