package com.flink.windows;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Demo04SessionWindows {
    public static void main(String[] args) throws Exception {
        /*
        会话窗口
        会话窗口的 assigner 会把数据按活跃的会话分组。 与滚动窗口和滑动窗口不同，会话窗口不会相互重叠，且没有固定的开始或结束时间。
         会话窗口在一段时间没有收到数据之后会关闭，即在一段不活跃的间隔之后。 会话窗口的 assigner 可以设置固定的会话间隔（session gap）或
         用 session gap extractor 函数来动态地定义多长时间算作不活跃。
         当超出了不活跃的时间段，当前的会话就会关闭，并且将接下来的数据分发到新的会话窗口。

         分为两类
         1. 基于事件时间的会话窗口:EventTimeSessionWindows
         2. 基于处理时间的会话窗口:ProcessingTimeSessionWindows
        * */

        // 设置如果10秒内没有数据，则认为会话结束
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> WordDS = env.socketTextStream("master", 8888);

        WordDS.map(line -> new Tuple2<String, Integer>(line.split(",")[0], 1)
                , Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .sum(1)
                .print();

        env.execute();

    }
}
