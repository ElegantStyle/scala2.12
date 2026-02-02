package com.flink.core;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo03socketDS {
    public static void main(String[] args) {

        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从 socket 读取数据
        env.socketTextStream("master",8888).print();

    }
}
