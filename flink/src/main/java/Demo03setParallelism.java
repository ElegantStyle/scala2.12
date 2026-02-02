import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Demo03setParallelism {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);

        DataStream<String> LineDS = env.socketTextStream("master", 8888);

        DataStream<String> flatMapDS = LineDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for (String word : s.split(",")) {
                    collector.collect(word);
                }
            }
        }).setParallelism(2);

        DataStream<Tuple2<String, Integer>> MapDS = flatMapDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).setParallelism(3);

        KeyedStream<Tuple2<String, Integer>, String> KeyByDS = MapDS.keyBy(f -> f.f0);
        KeyByDS.sum(1).print().setParallelism(5);

        env.execute();

    }
}
