package com.comen.flink;

import com.comen.flink.core.StreamEnvBuilder;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.ActionRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AAAA {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env =
                StreamEnvBuilder.builder()
                        .setCheckpointInterval(60000)
                        .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
                        .setCheckpointTimeout(60000L)
                        .setMinPauseBetweenCheckpoints(5000)
                        .setTolerableCheckpointFailureNumber(3)
                        .setMaxConcurrentCheckpoints(1)
                        .setDefaultRestartStrategy(5, Time.of(5, TimeUnit.MINUTES), Time.of(2, TimeUnit.MINUTES))
                        .setParallelism(1)
                        .build();


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.21.128")
                .port(3306)
                .databaseList("demo") // set captured database
                .tableList("demo.t1") // set captured table
                .username("root")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        List<HttpHost> httpHosts = new ArrayList<>(1);
        httpHosts.add(new HttpHost("192.168.8.80", 9200, "http"));
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                (ElasticsearchSinkFunction<String>) (s, runtimeContext, requestIndexer) ->
                        requestIndexer.add(createIndexRequest(s)));
        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setBulkFlushMaxSizeMb(1);
        esSinkBuilder.setBulkFlushInterval(5000L);
        esSinkBuilder.setRestClientFactory(
                (RestClientFactory)
                        restClientBuilder ->
                                restClientBuilder.setHttpClientConfigCallback(
                                        httpAsyncClientBuilder -> {
                                            httpAsyncClientBuilder.setKeepAliveStrategy(
                                                    (httpResponse, httpContext) ->
                                                            Duration.ofMinutes(5).toMillis());
                                            return httpAsyncClientBuilder;
                                        }));

        // enable checkpoint
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .addSink(esSinkBuilder.build())
                .setParallelism(1); // use parallelism 1 for sink to keep message ordering
        env.execute("Print MySQL Snapshot + Binlog");
    }

    private static ActionRequest createIndexRequest(String s) {
        return Requests.indexRequest()
                .index("demo_t1")
                .type("_doc")
                .source(s, XContentType.JSON);
    }
}
