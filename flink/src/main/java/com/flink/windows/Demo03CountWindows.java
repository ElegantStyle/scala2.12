package com.flink.windows;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo03CountWindows {
    public static void main(String[] args) throws Exception {
        /*
        计数窗口
        计数窗口的 assigner 分发元素到指定数量的窗口。计数窗口的大小是固定的，且各自范围之间不重叠。
        比如说，如果你指定了计数窗口的大小为 5，那么每 5 个元素就会有一个窗口被计算，且一个新的窗口被创建
        * */
        // 需求:每次当一个班级人数满5人时,就对改班级进行记录一次

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> WordDS = env.socketTextStream("master", 8888);

        WordDS.flatMap((line, collector) -> {
                    String[] lineDS = line.split(",");
                    collector.collect(Tuple2.of(lineDS[0], 1));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .countWindow(5)
                .sum(1)
                .print();

        env.execute();



    }
}
/*
文科六班
文科六班
理科六班
理科三班
理科五班
理科二班
文科六班
文科六班
文科六班
理科六班
理科一班
理科六班
理科三班
理科三班
理科三班
理科三班
理科一班
文科二班
理科五班
理科一班
 */
