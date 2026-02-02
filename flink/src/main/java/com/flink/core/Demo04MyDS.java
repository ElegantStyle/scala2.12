package com.flink.core;

import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.*;

public class Demo04MyDS {
    public static void main(String[] args) throws Exception {

        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 自定义数据源连接MySQL
        DataStreamSource<Tuple9<String,String,String,String,String,String,String,String,String>> dataStream =
            env.addSource(new MyJDBC("localhost", 3306, "comments", "wb", "root", "123456"));

        // 打印数据流
        dataStream.print();

        // 执行任务
        env.execute("Flink JDBC Source Example");
    }
}
class MyJDBC implements SourceFunction<Tuple9<String, String, String, String, String, String, String, String, String>> {

    String host;
    Integer port;
    String dbName;
    String tableName;
    String username;
    String password;

    public MyJDBC(String host, Integer port, String tableName, String dbName, String username, String password) {
        this.host = host;
        this.port = port;
        this.tableName = tableName;
        this.dbName = dbName;
        this.username = username;
        this.password = password;
    }

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple9<String, String, String, String, String, String, String, String, String>> sourceContext) throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {

            // 建立连接
            String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&characterEncoding=utf8",
                                     host, port, dbName);
            conn = DriverManager.getConnection(url, username, password);

            System.out.println("数据库连接成功!");

            // 执行查询
            stmt = conn.createStatement();
            rs = stmt.executeQuery(String.format("SELECT * FROM %s where region = '安徽' ", tableName));

            int count = 0;
            // 遍历结果集
            while (isRunning && rs.next()) {
                Tuple9<String, String, String, String, String, String, String, String, String> tuple = new Tuple9<>(
                    rs.getString("articleId") != null ? rs.getString("articleId") : "",
                    rs.getString("created_at") != null ? rs.getString("created_at") : "",
                    rs.getString("like_counts") != null ? rs.getString("like_counts") : "",
                    rs.getString("region") != null ? rs.getString("region") : "",
                    rs.getString("content") != null ? rs.getString("content") : "",
                    rs.getString("authorName") != null ? rs.getString("authorName") : "",
                    rs.getString("authorGender") != null ? rs.getString("authorGender") : "",
                    rs.getString("authorAddress") != null ? rs.getString("authorAddress") : "",
                    rs.getString("authorAvatar") != null ? rs.getString("authorAvatar") : ""
                );
                sourceContext.collect(tuple);
                count++;

                // 添加短暂延迟，避免过于频繁的数据收集
                Thread.sleep(100);
            }

            System.out.println("共收集了 " + count + " 条记录");
        } catch (SQLException e) {
            System.err.println("SQL异常: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            // 确保资源被正确关闭
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    System.err.println("ResultSet关闭失败: " + e.getMessage());
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    System.err.println("Statement关闭失败: " + e.getMessage());
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    System.err.println("Connection关闭失败: " + e.getMessage());
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        System.out.println("数据源已取消");
    }
}

