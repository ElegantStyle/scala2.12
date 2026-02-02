package com.flink.core;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class Demo02FliePathDS {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 老版本
//        env.readTextFile("./data/Flink/students.csv").print();

        // 新版本 使用 FlineSource读取文件
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                        new TextLineInputFormat(), new Path("./data/Flink/students.csv")
                )
                // 将有节流转换成无界流
                // monitorContinuously 监控文件变化,默认5s
                .monitorContinuously(Duration.ofSeconds(5))
                .build();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource").print();


        env.execute();
    }
}
