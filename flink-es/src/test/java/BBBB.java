package com.comen.flink;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class BBBB {
    @SneakyThrows
    public static void main(String[] args) {
//        val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        val tableEnvironment = TableEnvironment.create(bsSettings);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//线程数

        StreamTableEnvironment tev = StreamTableEnvironment.create(env);

        tev.executeSql("CREATE TABLE mysql_binlog (id INT NOT NULL, name STRING," +
                " PRIMARY KEY (`id`) NOT ENFORCED) " +
                "WITH ('connector' = 'mysql-cdc' , 'hostname' = '192.168.21.128', 'port' = '3306', " +
                " 'username' = 'root', " +
                "'password' = '123456', 'database-name' = 'demo', 'table-name' = 't1', 'server-time-zone'='Asia/Shanghai'," +
                "'scan.incremental.snapshot.enabled'= 'true')");
        tev.executeSql("CREATE TABLE t2(" +
                "    `id` int," +
                "    `name` string," +
                "    PRIMARY KEY (`id`) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'elasticsearch-7'," +
                "'hosts' = 'http://192.168.8.80:9200'," +
                "'index' = 'demo_t12'," +
                "'username' = ''," +
                "'password' = ''" +
                ")");
        tev.executeSql("INSERT INTO t2 SELECT id, name FROM mysql_binlog");
    }
}
