package com.ifeng.interest;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;
/*
* 2018.06.22
* 内容画像源：uimge.test5
* 内容兴趣人群包：interest.uimge_ilevel
* 20180622目前仅使用t1分析用户内容兴趣
* */
public class InterestUimg {

    private Cluster cluster;
    private Session session;
    private static final String INSERT_INTERESTUIMG = "insert into interest.uimge_ilevel " +
            "(uid,ua,utime,i600,i700,i800,i900,i1000,i1100,i1200,i1300,i1500,i1600,i1700,i1800) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

    private Session getSession() {
        return session;
    }

    private void connect(String[] node, int port) {
        SocketOptions so = new SocketOptions().setReadTimeoutMillis(30000).setConnectTimeoutMillis(30000);

        PoolingOptions poolingOptions= new PoolingOptions()
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
        this.session = cluster.connect("groups");
    }

    private Map<String,String> loadMap(String filePath) {
        Map<String,String> alline=new HashMap<>();
        try {
            InputStream is=this.getClass().getResourceAsStream(filePath);
            BufferedReader br=new BufferedReader(new InputStreamReader(is));
            String tempString;
            while ((tempString=br.readLine())!=null) {
                alline.put(tempString.split("=")[0],tempString.split("=")[1]);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return alline;
    }

    private void loadData(String query, Map<String,String> uimgMap) {

        ResultSet resultSet = getSession().execute(query);
        PreparedStatement prepareStatement = session.prepare(INSERT_INTERESTUIMG);
        int counter = 0;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            try {
            for (Row row : resultSet) {
                String t1 = row.getString("t1");
                String uid = row.getString("uid");
                String ua = row.getString("ua");
                String utime = row.getString("utime");
                HashMap<String,Float> insertMap =new HashMap<>();
                if(t1 == null) continue;
                String[] t1String = t1.split("#");
                for (String s : t1String) {
                    String[] str = s.split("_");
                    String k = str[0];
                    System.out.println("str[0]: " + str[0]);
                    if (str.length == 4 && uimgMap.containsKey(k)) {
                        float v = Float.parseFloat(str[3]);
                        System.out.println("v: "+v);
                        String tag = uimgMap.get(k);
                        System.out.println("tag: "+tag);
                        if (!insertMap.containsKey(tag) || (insertMap.containsKey(tag) && insertMap.get(tag) < v)) {
                            insertMap.put(tag, v);
                        }
                    }
                }

                float i600 = (insertMap.containsKey("600") ? insertMap.get("600") : 0);
                float i700 = (insertMap.containsKey("700") ? insertMap.get("700") : 0);
                float i800 = (insertMap.containsKey("800") ? insertMap.get("800") : 0);
                float i900 = (insertMap.containsKey("900") ? insertMap.get("900") : 0);
                float i1000 = (insertMap.containsKey("1000") ? insertMap.get("1000") : 0);
                float i1100 = (insertMap.containsKey("1100") ? insertMap.get("1100") : 0);
                float i1200 = (insertMap.containsKey("1200") ? insertMap.get("1200") : 0);
                float i1300 = (insertMap.containsKey("1300") ? insertMap.get("1300") : 0);
                float i1500 = (insertMap.containsKey("1500") ? insertMap.get("1500") : 0);
                float i1600 = (insertMap.containsKey("1600") ? insertMap.get("1600") : 0);
                float i1700 = (insertMap.containsKey("1700") ? insertMap.get("1700") : 0);
                float i1800 = (insertMap.containsKey("1800") ? insertMap.get("1800") : 0);

                BoundStatement bindStatement = new BoundStatement(prepareStatement)
                        .bind(uid,ua,utime,i600,i700,i800,i900,i1000,i1100,i1200,i1300,i1500,i1600,i1700,i1800);
                session.execute(bindStatement);

                if (counter % 1000000 == 0) {
                    System.out.println("interest uimg: " + counter +
                            " Time: " + dateFormat.format(new Date()));
                }
                counter++;
            }
            System.out.println("interest uimg: " + counter +
                    " Time: " + dateFormat.format(new Date()));

        } catch (OperationTimedOutException e) {
            System.out.println(e.getMessage() + " " + dateFormat.format(new Date()));
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        InterestUimg client = new InterestUimg();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155", "10.80.25.155"};
        int port = 9042;
        client.connect(contact_points, port);
        Map<String,String> listW1 = client.loadMap("/uimgTag");
//        String query1 = "select uid, ua, t1, utime from uimg.test5";
        String query2 = "select uid, ua, utime,t1 from interest.uimge_values";
        client.loadData(query2, listW1);
        client.session.close();
        client.close();
    }
}



