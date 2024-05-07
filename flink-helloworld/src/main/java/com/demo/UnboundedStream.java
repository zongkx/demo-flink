package com.demo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * 无界流 无界数据流有一个开始但是没有结束，它们不会在生成时终止并提供数据，必须连续处理无界流，也就是说必须在获取后立即处理event。对于无界数据流我们无法等待所有数据都到达，因为输入是无界的，并且在任何时间点都不会完成。处理无界数据通常要求以特定顺序（例如事件发生的顺序）获取event，以便能够推断结果完整性。
 * 比如 kafka
 */
public class UnboundedStream {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 下载  https://eternallybored.org/misc/netcat/netcat-win32-1.11.zip  cmd进入该目录， nc -L -p 9000 -v 即可创建该端口
        // 监听端口
        DataStreamSource<String> source = env.socketTextStream("localhost", 9000);
        source.print();
        env.execute();

    }

}
