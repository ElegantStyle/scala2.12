package com.flink.windows;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Demo01TumblingWindows {
    public static void main(String[] args) throws Exception {
        // 滚动窗口
        /*
         * 滚动窗口的 assigner 分发元素到指定大小的窗口。滚动窗口的大小是固定的，且各自范围之间不重叠。
         * 比如说，如果你指定了滚动窗口的大小为 5 分钟，那么每 5 分钟就会有一个窗口被计算，且一个新的窗口被创建
         *
         * */
        // 滚动窗口分为两种
        // 基于处理时间的滚动窗口:TumblingProcessingTimeWindows
//        TumblingProcessingTimeWindows();
        // 基于事件时间的滚动窗口:TumblingEventTimeWindows
        TumblingEventTimeWindows();
        /*
        2026-02-04 19:55:00
        a,1770206100000,1
        a,1770206101000,1
        a,1770206102000,1
        a,1770206103000,1
        a,1770206104000,1
        a,1770206105000,1
        a,1770206106000,1
        a,1770206107000,1
        a,1770206108000,1
        a,1770206109000,1
        a,1770206110000,1
        a,1770206111000,1
        * */

    }

    public static void TumblingProcessingTimeWindows() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> WordDS = env.socketTextStream("master", 8888);
        WordDS.flatMap((line, collector) -> {
                    String[] lineDS = line.split(",");
                    collector.collect(Tuple2.of(lineDS[0], 1));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();

        env.execute();

    }

    public static void TumblingEventTimeWindows() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> WordDS = env.socketTextStream("master", 8888);
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> WordMap = WordDS.map(line -> new Tuple3<String, Long, Integer>(line.split(",")[0],
                Long.parseLong(line.split(",")[1]), 1), Types.TUPLE(Types.STRING, Types.LONG, Types.INT));
        // 告诉Flink,数据源的时间戳是哪一列

        WordMap.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f1)
                ).keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(2)
                .map(line -> line.f0 + "," + line.f2)
                .print();
        env.execute();

    }
}
