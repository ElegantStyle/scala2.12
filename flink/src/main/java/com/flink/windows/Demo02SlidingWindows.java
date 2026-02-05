package com.flink.windows;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Demo02SlidingWindows {
    public static void main(String[] args) throws Exception {
        /*
        滑动窗口
       定义是:与滚动窗口类似，滑动窗口的 assigner 分发元素到指定大小的窗口，窗口大小通过 window size 参数设置。
       滑动窗口需要一个额外的滑动距离（window slide）参数来控制生成新窗口的频率。 因此，如果 slide 小于窗口大小，
       滑动窗口可以允许窗口重叠。这种情况下，一个元素可能会被分发到多个窗口。
       比如说，你设置了大小为 10 分钟，滑动距离 5 分钟的窗口，你会在每 5 分钟得到一个新的窗口， 里面包含之前 10 分钟到达的数据

       滑动窗口分为:事件时间的滑动窗口(slidingProcessingTimeWindows)和 处理时间的滑动窗口(slidingEventTimeWindows)
        *
        */
//        slidingProcessingTimeWindows();
        slidingEventTimeWindows();



    }
    public static void slidingProcessingTimeWindows() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> WordDS = env.socketTextStream("master", 8888);
        WordDS.flatMap((line, collector) -> {
                    String[] lineDS = line.split(",");
                    collector.collect(Tuple2.of(lineDS[0], 1));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .sum(1)
                .print();
        env.execute();
    }

    public static void slidingEventTimeWindows() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> WordDS = env.socketTextStream("master", 8888);
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> WordMap = WordDS.map(line -> new Tuple3<String, Long, Integer>
                (line.split(",")[0], Long.parseLong(line.split(",")[1]), 1),
                Types.TUPLE(Types.STRING, Types.LONG, Types.INT));

        // 告诉Flink,数据源的时间戳是哪一列
        WordMap.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.f1))
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .sum(2)
                .map(line -> line.f0 + "," + line.f2)
                .print();

        env.execute();
    }

}
/*
2026-02-04 19:55:00
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
