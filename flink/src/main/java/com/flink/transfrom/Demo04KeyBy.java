package com.flink.transfrom;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo04KeyBy {
    public static void main(String[] args) throws Exception {
        // KeyBy 算子:对数据源进行分组 , 按照指定的字段和哈希值对数据进行分组
        // 从 DataStream -> KeyedStream

        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建批数据流
        DataStreamSource<String> stuDS = env.readTextFile("./data/Flink/students.csv");

        // 设置使用批
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 使用 flatMap 算子先实现切分
        stuDS.flatMap((line, collector) -> {
                    String[] stuWord = line.split(",");
                    collector.collect(new FlatMapClass(stuWord[0], stuWord[1], Integer.parseInt(stuWord[2]), stuWord[3], stuWord[4]));
                }, Types.GENERIC(FlatMapClass.class))
                // 按照班级进行分组
                .keyBy(FlatMapClass::getClazz)
                .print();

        // 启动任务
        env.execute();


    }
}
