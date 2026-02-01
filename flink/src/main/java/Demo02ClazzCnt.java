import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo02ClazzCnt {
    public static void main(String[] args) throws Exception{
        // 有界流数据处理
        // 创建 Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(1);
        // 设置间隔时间
        env.setBufferTimeout(100);

        /*
         * 设置运行模式，共两种模式：
         * 1、批处理模式：BATCH，只能用于有界流，只会返回最终的结果
         * 2、流处理模式：STREAMING，可以用于有界流、无界流，会返回不断变化的结果
         */
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // 读取文件
        DataStreamSource<String> stuLine = env.readTextFile("./data/Flink/students.csv");

        // 使用 Map 函数进行处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = stuLine.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s.split(",")[4], 1);
            }
        });

        // 进行分组处理
        KeyedStream<Tuple2<String, Integer>, String> KeyByDS = mapDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        KeyByDS.sum(1).print();

        env.execute();


    }
}
