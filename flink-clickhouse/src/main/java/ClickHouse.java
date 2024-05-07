import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author zongkxc
 */


public class ClickHouse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        // 配置kafka信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "39.97.243.43:9092");
        properties.setProperty("group.id", "demo");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<String>("topic1", new SimpleStringSchema(), properties);
        myConsumer.setStartFromLatest();

        DataStream<String> stream = env.addSource(myConsumer).setParallelism(1);

        SingleOutputStreamOperator<User> dataStream = stream.map(new MapFunction<String, User>() {
            @Override
            public User map(String data) throws Exception {
                String[] split = data.split(",");
                return User.of((split[0]), split[1], (split[2]));
            }
        });

        // sink
        String sql = "INSERT INTO default.my3 VALUES (?,?,?)";
        MyClickHouseSink jdbcSink = new MyClickHouseSink(sql);
        dataStream.addSink(jdbcSink);
        dataStream.print();

        env.execute("clickhouse sink test");
    }

}
