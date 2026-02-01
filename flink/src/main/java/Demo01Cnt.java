import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Demo01Cnt {
    public static void main(String[] args) throws Exception {
        // 创建 flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1. 创建数据源：从Socket读取数据
        DataStreamSource<String> lineDS = env.socketTextStream("master", 8888);
        // 2. 数据转换处理：分割单词并转换为(word, 1)键值对
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOneDS = lineDS.flatMap( (line, out) -> {
            String[] split = line.split(",");
            for (String word : split) {
                out.collect(Tuple2.of(word, 1));
            }
        });
        // 3. 分组：按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> wordKeyDS = wordOneDS.keyBy(t -> t.f0);
        // 4. 聚合：计算每个单词出现的次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCountDS = wordKeyDS.sum(1);
        // 5. 结果输出
        wordCountDS.print();
        // 6. 启动Flink作业
        env.execute("Flink WordCount Demo");
    }
}