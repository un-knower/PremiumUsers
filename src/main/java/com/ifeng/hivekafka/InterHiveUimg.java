package com.ifeng.hivekafka;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import uimge.util.LoadMap;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 2018.06.26
 * 内容画像源：hive2.uimge.uimge_values
 * 内容兴趣人群包：interest.uimge_ilevel
 * 20180622目前仅使用t1分析用户内容兴趣
 */

public class InterHiveUimg {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://10.90.9.112:9083/uimge";
    private static String user = "";
    private static String password = "";

    private static Connection conn = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;

    private Cluster cluster;
    private Session session;

    private static final String INSERT_ILEVEL = "insert into interest.uimge_ilevel " +
            "(uid,ua,i600,i700,i800,i900,i1000,i1100,i1200,i1300,i1600,i1700,i1800) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);";

    private void connectHive() {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, user, password);
            if(conn == null) System.out.println("conn is null!");
            stmt = conn.createStatement();
            if(stmt == null) System.out.println("stmt is null!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void connectCass(String[] node, int port) {
        SocketOptions so = new SocketOptions().setReadTimeoutMillis(50000).setConnectTimeoutMillis(50000);

        PoolingOptions poolingOptions = new PoolingOptions()
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 64)//每个连接最多允许64个并发请求
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 2)//和集群里的每个机器都至少有2个连接
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 6);//和集群里的每个机器都最多有6个连接
        QueryOptions queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
        RetryPolicy retryPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;

        cluster = Cluster.builder()
                .addContactPoints(node)
                .withSocketOptions(so)
                .withPoolingOptions(poolingOptions)
                .withQueryOptions(queryOptions)
                .withRetryPolicy(retryPolicy)
                .withPort(port)
                .build();

        cluster.getConfiguration().getQueryOptions().setFetchSize(50);
        this.session = cluster.connect("interest");
    }

    private void loadData(String query, Map<String, String> uimgMap) {
        int counter = 0;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        PreparedStatement prepareStatement = session.prepare(INSERT_ILEVEL);

        try {
            rs = stmt.executeQuery(query);
            while (rs.next()) {
                String uid = rs.getString("uid");
                String ua = rs.getString("ua");
                String t1List = rs.getString("t1");
                HashMap<String, Float> insertMap = new HashMap<>();
                if (t1List.isEmpty()) continue;

                for (String s : t1List.split("#")) {
                    String[] str = s.split("_");
                    String k = str[0];
                    if (str.length == 4 && uimgMap.containsKey(k)) {
                        float v = Float.parseFloat(str[3]);
                        String tag = uimgMap.get(k);
                        System.out.println("tag: " + tag + " v: " + v);
                        if (!insertMap.containsKey(tag) || (insertMap.containsKey(tag) && insertMap.get(tag) < v)) {
                            insertMap.put(tag, v);
                        }
                    }
                }

                if (insertMap.isEmpty()) continue;

                float i600 = (insertMap.containsKey("600") ? insertMap.get("600") : 0);
                float i700 = (insertMap.containsKey("700") ? insertMap.get("700") : 0);
                float i800 = (insertMap.containsKey("800") ? insertMap.get("800") : 0);
                float i900 = (insertMap.containsKey("900") ? insertMap.get("900") : 0);
                float i1000 = (insertMap.containsKey("1000") ? insertMap.get("1000") : 0);
                float i1100 = (insertMap.containsKey("1100") ? insertMap.get("1100") : 0);
                float i1200 = (insertMap.containsKey("1200") ? insertMap.get("1200") : 0);
                float i1300 = (insertMap.containsKey("1300") ? insertMap.get("1300") : 0);
                float i1600 = (insertMap.containsKey("1600") ? insertMap.get("1600") : 0);
                float i1700 = (insertMap.containsKey("1700") ? insertMap.get("1700") : 0);
                float i1800 = (insertMap.containsKey("1800") ? insertMap.get("1800") : 0);

                BoundStatement bindStatement = new BoundStatement(prepareStatement)
                        .bind(uid, ua, i600, i700, i800, i900, i1000, i1100, i1200, i1300, i1600, i1700, i1800);
                session.execute(bindStatement);

            }


            if (counter % 100000 == 0) {
                System.out.println("interest hive uimg: " + counter +
                        " Time: " + dateFormat.format(new Date()));
            }
            counter++;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        InterHiveUimg client = new InterHiveUimg();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155", "10.80.25.155"};
        int port = 9042;
        System.out.println("start!");
        client.connectHive();
        client.connectCass(contact_points, port);
        Map<String, String> listW1 = new LoadMap().load("/uimgTag");
        String query1 = "select uid, ua, t1 from uimge.uimge_values limit 100";
        client.loadData(query1, listW1);

    }
}



