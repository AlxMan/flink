package com.wxc.java.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        String inPath = "data/wordcount.txt";
        String outPath = "output/wordcount_scala.csv";

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = executionEnvironment.readTextFile(inPath);

        DataSet<Tuple2<String,Integer>> dataSet = text.flatMap(new LineSplitter()).groupBy(0).sum(1);
        dataSet.writeAsCsv(outPath,"\n"," ").setParallelism(1);

        executionEnvironment.execute("wordCount batch process");
    }
    static class LineSplitter implements FlatMapFunction<String,Tuple2<String,Integer>> {

        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word:line.split(" ")){
                collector.collect(new Tuple2<String, Integer>(word,1));
            }
        }
    }
}
