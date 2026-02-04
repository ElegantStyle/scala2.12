package com.flink.transfrom;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Demo06Window {
    public static void main(String[] args) throws Exception {
        // Window 窗口操作:对最近N秒/分钟的数据进行聚合处理

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用流处理方式
        DataStreamSource<String> ScoStuDS = env.socketTextStream("master", 8888);

        // 使用FlatMap 算子先处理
        ScoStuDS.flatMap((line, collector) -> {
                    String[] lineDS = line.split(",");
                    collector.collect(Tuple2.of(lineDS[4], 1));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();
        env.execute();
    }
}
