package com.flink.windows;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class Demo05Watermark {
    public static void main(String[] args) throws Exception {
        // 使用水位线和事件时间连用
        /*
        什么是水位线？
        在Flink中，用来衡量事件时间进展的标记，就被称作“水位线”（Watermark）
        水位线分为三类:
        1、水位线前移策略： forBoundedOutOfOrderness
        2、连续单调递增的水位线策略： forMonotonousTimestamps，等价于水位线前移0s
        3、不生成水位线： noWatermarks
        * */

        // 首先创建数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> WordDS = env.socketTextStream("master", 8888);
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> WordMap = WordDS.map(line -> new Tuple3<String, Long, Integer>(line.split(",")[0],
                Long.parseLong(line.split(",")[1]), 1), Types.TUPLE(Types.STRING, Types.LONG, Types.INT));
        // 告诉Flink,数据源的时间戳是哪一列
        WordMap.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Long, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.f1)
                ).keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(3)))
                .sum(2)
                .map(line -> line.f0 + "," + line.f2)
                .print();

        env.execute();
    }
}


/*
a,1770206100000
a,1770206101000
a,1770206102000
a,1770206103000
a,1770206104000
a,1770206105000
a,1770206106000
a,1770206107000
a,1770206108000
a,1770206109000
a,1770206110000
a,1770206111000
a,1770206112000
a,1770206113000
a,1770206114000
a,1770206115000
a,1770206116000
*/