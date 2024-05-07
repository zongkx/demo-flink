package com.comen.flink;

import lombok.val;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class CCCC {
    public static void main(String[] args) {
        val bsSettings = EnvironmentSettings.newInstance().inBatchMode().build();
        val tev = TableEnvironment.create(bsSettings);
        tev.executeSql("CREATE TABLE mysql_binlog (id INT NOT NULL, name STRING," +
                " PRIMARY KEY (`id`) NOT ENFORCED) " +
                "WITH ('connector' = 'jdbc','url' = 'jdbc:mysql://192.168.21.128:3306/demo'," +
                " 'driver' = 'com.mysql.cj.jdbc.Driver', " +
                " 'username' = 'root', " +
                "'password' = '123456', 'table-name' = 't1')");
        tev.executeSql("CREATE TABLE t2(" +
                "    `id` int," +
                "    `name` string," +
                "    PRIMARY KEY (`id`) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'elasticsearch-7'," +
                "'hosts' = 'http://192.168.8.80:9200'," +
                "'index' = 'demo_t13'," +
                "'username' = ''," +
                "'password' = ''" +
                ")");
        tev.executeSql("INSERT INTO t2 SELECT id, name FROM mysql_binlog");
    }
}
