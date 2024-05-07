import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class MyClickHouseSink extends RichSinkFunction<User> {
    Connection connection = null;

    String sql;

    public MyClickHouseSink(String sql) {
        this.sql = sql;
    }
 
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = this.getConn("192.168.203.128", 8123, "default");
    }
 
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }
 
    @Override
    public void invoke(User user, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, user.id);
        preparedStatement.setString(2, user.name);
        preparedStatement.setString(3, user.age);
        preparedStatement.addBatch();
 
        long startTime = System.currentTimeMillis();
        int[] ints = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
    }

    public Connection getConn(String host, int port, String database) throws SQLException, ClassNotFoundException {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        String  address = "jdbc:clickhouse://" + host + ":" + port + "/" + database;
        this.connection = DriverManager.getConnection(address);
        return connection;
    }
}