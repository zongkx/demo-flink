import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author zongkxc
 */


public class MongoCDC {
        public static void main(String[] args) throws Exception {
            SourceFunction<String> sourceFunction = MongoDBSource.<String>builder()
                    .hosts("39.97.243.43:27017")
                    .username("test")
                    .password("123456789")
                    .databaseList("test") // set captured database, support regex
                    .collectionList("test.user") //set captured collections, support regex
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.addSource(sourceFunction)
                    .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

            env.execute();
        }
    }
