package com.flink.state;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Demo01State {
    public static void main(String[] args) throws Exception {

        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建数据源
        DataStreamSource<String> WordDS = env.socketTextStream("master", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> WordMap =
                WordDS.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
                    // 实现 Map 算子
                    @Override
                    public void processElement(String word, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of(word, 1));
                    }
                }, Types.TUPLE(Types.STRING, Types.INT));
        
        WordMap.keyBy(f->f.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    // 定义一个字典 HashMap
                    HashMap<String, Integer> WordHashMap = new HashMap<>();
                    @Override
                    public void processElement(Tuple2<String, Integer> word, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        if (!WordHashMap.containsKey(word.f0)){
                            WordHashMap.put(word.f0, 1);
                        } else{
                            Integer WordCNT = WordHashMap.get(word.f0);
                            WordHashMap.put(word.f0,WordCNT+1);
                        }
                        out.collect(Tuple2.of(word.f0, WordHashMap.get(word.f0)));
                    }
                })
                .print();

        env.execute();
    }    
}
