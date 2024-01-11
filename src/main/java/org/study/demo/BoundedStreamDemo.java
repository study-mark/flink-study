package org.study.demo;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @program: flinkgo
 * @ClassName StreamDemo
 * @description:
 * @author: Mark
 * @create: 2024.01.11
 **/
public class BoundedStreamDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("inputSource/hello.txt");

        SingleOutputStreamOperator<Tuple2<String, Long>> result = source.flatMap((String v, Collector<Tuple2<String, Long>> out) -> {
            String[] s = v.split(" ");
            for (String word : s) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //分组
        KeyedStream<Tuple2<String, Long>, String> groupby = result.keyBy(data -> data.f0);

        //求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = groupby.sum(1);

        sum.print();

        //执行
        env.execute();
    }

}
