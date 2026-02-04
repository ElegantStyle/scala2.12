package com.flink.transfrom;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo03Filter {
    public static void main(String[] args) throws Exception {
        /*
        * Filter 算子,对满足条件的数据进行过滤
        * */

        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建数据源,使用批处理
        DataStreamSource<String> socDS = env.readTextFile("./data/Flink/students.csv");

        // 设置模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        // 使用 flatmap 算子对数据进行转换,然后调用 filter 只输出为女生的数据

        SingleOutputStreamOperator<FlatMapClass> FlatMapDS = socDS.flatMap((line, collector) -> {
            String[] lineDS = line.split(",");
            FlatMapClass flatMapClass = new FlatMapClass(lineDS[0], lineDS[1], Integer.parseInt(lineDS[2]), lineDS[3], lineDS[4]);
            collector.collect(flatMapClass);
        }, Types.GENERIC(FlatMapClass.class));

        FlatMapDS.filter(flatMapClass -> flatMapClass.gender.equals("女")).print();


        // 启动任务
        env.execute();

    }
}
