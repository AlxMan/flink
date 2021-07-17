package com.wxc.java.streamsource;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.ArrayList;

public class StreamFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // DataStreamSource<String> data = env.fromElements("spark", "flink");
        env.setParallelism(4);
        ArrayList<People> peopleList = new ArrayList<People>();
        peopleList.add(new People("lucas", 18));
        peopleList.add(new People("jack", 30));
        peopleList.add(new People("jack", 40));
        DataStreamSource<People> data = env.fromCollection(peopleList);
        // DataStreamSource<People> data = env.fromElements(new People("lucas", 18), new People("jack", 30), new People("jack", 40));
        SingleOutputStreamOperator<People> filtered = data.filter(new FilterFunction<People>() {
            public boolean filter(People people) throws Exception {
                return people.age > 20;
            }
        });
        filtered.print();
        env.execute();
    }

    public static class People{
        public String name;
        public Integer age;
        public People(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public Integer getAge() {
            return age;
        }
        public void setAge(Integer age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "People{" + "name='" + name + '\'' + ", age=" + age + '}';
        }
    }
}