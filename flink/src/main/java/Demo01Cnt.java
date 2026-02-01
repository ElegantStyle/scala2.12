import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class Demo01Cnt {
    public static void main(String[] args) throws Exception {
        // 创建 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 设置并行度,默认等于CPU的核数
        env.setParallelism(2);
        // Flink 会优化上下数据传输的方式(以32KB作为一批,间隔默认100ms),数据到了下游还是逐条处理
        env.setBufferTimeout(100);

        // 创建数据源
        DataStream<String> lineDS = env.socketTextStream("master", 8888);

        // 处理数据源
        // 使用FlatMap 算子将一行转换多行
        DataStream<String> splitDS = lineDS.flatMap((FlatMapFunction<String, String>) (s, collector) -> {
            for (String word : s.split(",")) {
                collector.collect(word);
            }
        });

        // 使用 Map 算子将数据转换成二元组(单词,1)
        DataStream<Tuple2<String, Integer>> MapDS = splitDS.map((MapFunction<String, Tuple2<String, Integer>>) s -> new Tuple2<>(s, 1));
        
        // 使用 keyBy 进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyByDS = MapDS.keyBy((KeySelector<Tuple2<String, Integer>, String>) stringIntegerTuple2 -> stringIntegerTuple2.f0);

         // 完成单词统计
        keyByDS.sum(1).print();
        env.execute();


    }
}