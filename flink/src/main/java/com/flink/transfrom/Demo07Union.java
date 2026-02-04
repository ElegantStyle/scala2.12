package com.flink.transfrom;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo07Union {
    public static void main(String[] args) throws Exception {
        // Union 算子,将多个数据源的数据进行合并

        // 使用两个批数据进行演示
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 第一个数据源
        DataStreamSource<String> stuDS1 = env.readTextFile("./data/Flink/students.csv");
        // 第二个数据源
        DataStreamSource<String> stuDS2 = env.readTextFile("./data/Flink/students.csv");

        SingleOutputStreamOperator<Tuple> stuKeyBy1 = stuDS1.flatMap((line, collector) -> {
                    String[] lineDS = line.split(",");
                    collector.collect(Tuple2.of(lineDS[4], 1));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1);

        SingleOutputStreamOperator<Tuple> stuKeyBy2 = stuDS2.flatMap((line, collector) -> {
                    String[] lineDS = line.split(",");
                    collector.collect(Tuple2.of(lineDS[4], 1));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1);

        stuKeyBy1.union(stuKeyBy2)
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();

    }
}
