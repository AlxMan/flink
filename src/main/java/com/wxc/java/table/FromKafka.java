package com.wxc.java.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * FlinkTable从kafka上获取数据
 */
public class FromKafka {
    public static void main(String[] args) throws Exception {
        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //tEnv
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database")
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.connect(
                new Kafka()
                .version("universal")
                .topic("animal")
                .startFromEarliest()
                .property("bootstrap.servers","linux122:9092")
        )
                .withFormat(new Csv())
                .withSchema(
                        new Schema().field("name", DataTypes.STRING())
                )
                .createTemporaryTable("animalTable");

        String sql = "select * from animalTable";
        Table resultTable = tEnv.sqlQuery(sql);
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tEnv.toRetractStream(resultTable, Row.class);

        tuple2DataStream.print();
        env.execute();

    }
}
