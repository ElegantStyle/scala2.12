package com.flink.transfrom;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo05Reduce {
    public static void main(String[] args) throws Exception {
        // Reduce 算子: 对数据进行聚合处理

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置数据源
        DataStreamSource<String> stuDS = env.readTextFile("./data/Flink/students.csv");

        // 设置处理模式为批处理
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 完成班级人数的统计
        stuDS.flatMap((line, collector) -> {
                    String[] lineDS = line.split(",");
                    collector.collect(new Tuple2<>( lineDS[4], 1));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1)
                .print();
        // 启动任务
        env.execute();

    }
}
