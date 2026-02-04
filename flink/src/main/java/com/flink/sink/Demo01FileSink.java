package com.flink.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

public class Demo01FileSink {
    public static void main(String[] args) throws Exception {

        // 完成班级人数的统计,并且将结果写入

        // 创建 Flink 对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置运行模式为 Automatic
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // 设置并行度为1
        env.setParallelism(1);
        // 读取数据源
        DataStreamSource<String> StuDS = env.readTextFile("./data/Flink/students.csv");

        // 使用 Map 算子
        SingleOutputStreamOperator<Tuple2> ClazzSum = StuDS.map(line -> new Tuple2(line.split(",")[4], 1), Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1);

        SingleOutputStreamOperator<String> StringSum = ClazzSum.map(line -> line.f0 + "," + line.f1);

        // 写入data/Flink/output目录中
        FileSink<String> sink = FileSink
                .forRowFormat(new Path("./data/Flink/output"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(15))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build())
                .build();

        StringSum.sinkTo(sink);

        // 启动任务
        env.execute();


    }
}
