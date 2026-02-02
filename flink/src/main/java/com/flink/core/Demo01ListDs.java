package com.flink.core;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.ArrayList;

public class Demo01ListDs {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建 List
        ArrayList<String> ArrList = new ArrayList<>();
        ArrList.add("java");
        ArrList.add("java");
        ArrList.add("java");
        ArrList.add("java");
        ArrList.add("java");
        ArrList.add("java");
        ArrList.add("java");

        // 读取 List 中的数据
        env.fromCollection(ArrList).print();

        env.execute();

    }
}
