package com.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 有界流 :有界数据流有明确定义的开始和结束，可以在执行任何计算之前通过获取所有数据来处理有界流，处理有界流不需要有序获取，因为可以始终对有界数据集进行排序，有界流的处理也称为批处理。
 * @author zongkxc
 */


public class BoundedStream {
    public static void main(String[] args) throws Exception {
        // 创建有界流环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.fromElements(
                "Flink flink flink ",
                "spark spark spark",
                "Spark Spark Spark");

        AggregateOperator<Tuple2<String, Integer>> sum = text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String o, Collector collector) throws Exception {
                        String[] split = o.toLowerCase().split("\\W+");

                        for (String t: split){
                            collector.collect(new Tuple2<String, Integer>(t,1));
                        }
                    }
                })
                .groupBy(0)
                .sum(1);

        sum.print();
    }

}
