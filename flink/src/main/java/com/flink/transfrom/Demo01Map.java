package com.flink.transfrom;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo01Map {
    public static void main(String[] args) throws Exception {
        /*
        * map 算子:对数据源进行转换的算子,传入一行数据,返回一行数据
        * */

        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建数据源
        DataStreamSource<String> sockDS = env.socketTextStream("master", 8888);

        // 使用 map算子 对数据进行转换处理
        sockDS.map(s -> Tuple2.of(s,1), Types.TUPLE(Types.STRING, Types.INT)).print();

        // 启动任务
        env.execute();


    }
}
