package org.study.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @program: flinkgo
 * @ClassName BatchDemo
 * @description:
 * @author: Mark
 * @create: 2024.01.11
 **/
public class BatchDemo {

    public static void main(String[] args) throws Exception {
        //创建flink环境
        ExecutionEnvironment flink_env = ExecutionEnvironment.getExecutionEnvironment();

        //读取文件
        String filePath = "inputSource/hello.txt";
        DataSource<String> source = flink_env.readTextFile(filePath);

        //转换为二元组
        FlatMapOperator<String, Tuple2<String, Long>> result = source.flatMap((String v, Collector<Tuple2<String, Long>> out) -> {
            String[] s = v.split(" ");
            for (String word : s) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //分组
        UnsortedGrouping<Tuple2<String, Long>> groupByResult = result.groupBy(0);

        //聚合
        AggregateOperator<Tuple2<String, Long>> sum = groupByResult.sum(1);

        sum.print();
    }
}
