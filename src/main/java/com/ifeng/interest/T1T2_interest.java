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

/**
 *
 */


public class T1T2_interest {

    private Cluster cluster;
    private Session session;
    private static final String INSERT_AllKeywordsT1T2 = "insert into groups.all_keywords_t1t2 " +
            "(uid, keywords, row_num) VALUES (?,?,?);";

    private Session getSession() {
        return session;
    }

    private void connect(String[] node, int port) {
        SocketOptions so = new SocketOptions().setReadTimeoutMillis(50000).setConnectTimeoutMillis(50000);

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
        this.session = cluster.connect("interest");
    }

    private List<String> loadKeyword(String filePath) {
        List<String> alline=new ArrayList<>();
        try {
            InputStream is=this.getClass().getResourceAsStream(filePath);
            BufferedReader br=new BufferedReader(new InputStreamReader(is));
            String tempString;
            while ((tempString=br.readLine())!=null) {
                alline.add(tempString);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return alline;
    }

    private void loadData(String query, List<String> list1, List<String> list2) {

        ResultSet resultSet = getSession().execute(query);
        PreparedStatement prepareStatement = session.prepare(INSERT_AllKeywordsT1T2);
        int counter = 0;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        try {
            for (Row row : resultSet) {
                List<String> t1List = row.getList("t1", String.class);
                List<String> t2List = row.getList("t2", String.class);
                String user_key = row.getString("uid");
                List<String> wordList = new ArrayList<>();

                for (String s : t1List) {
                    String t1String;
                    String str1 = s.split("_")[0];
                    int len = s.split("_").length;
                    if (!(str1.equals("null")) && !(str1.equals("$")) && !(str1.equals("")) && !(str1.equals(" "))) {
                        if (list1.contains(str1)) {
                            int index = list1.indexOf(str1) + 1;
                            t1String = index + ":" + s.split("_")[len-1];
                            wordList.add(t1String);
                        }
                    }
                }

                for (String s : t2List) {
                    String t2String;
                    String str2 = s.split("_")[0];
                    int len = s.split("_").length;
                    if (!(str2.equals("null")) && !(str2.equals("$")) && !(str2.equals("")) && !(str2.equals(" "))) {
                        if (list2.contains(str2)) {
                            int index = list2.indexOf(str2)+ list1.size() + 1;
                            t2String = index + ":" + s.split("_")[len-1];
                            wordList.add(t2String);
                        }
                    }
                }

                if(wordList.size() == 0) {
                    continue;
                }
                // 排序主题编号，变成LibSVM数据
                Collections.sort(wordList, new Comparator<String>() {
                    public int compare(String s1, String s2) {
                        String o1 = s1.split(":")[0];
                        String o2 = s2.split(":")[0];
                        return (Integer.parseInt(o1) - Integer.parseInt(o2));
                    }
                });

                // 去除一个用户的重复主题,比之前耗时少
                for (int i = 0; i < wordList.size() - 1; i++) {
                    String t2 = wordList.get(i).split(":")[0];
                    String t1 = wordList.get(i + 1).split(":")[0];
                    if (t1.equals(t2)) {
                        wordList.remove(i + 1);
                        i--;
                    }
                }

                StringBuilder stringBuilder = new StringBuilder();
                if (!(wordList.isEmpty())) {
                    for (String str : wordList)
                        stringBuilder.append(" ").append(str);
                }

                BoundStatement bindStatement = new BoundStatement(prepareStatement)
                        .bind(user_key,stringBuilder.toString(), counter);
                session.execute(bindStatement);

                if (counter % 100000 == 0) {
                    System.out.println("interest_t1t2: " + counter +
                            " Time: " + dateFormat.format(new Date()));
                }
                counter++;
            }
            System.out.println("interest_t1t2: " + counter +
                    " Time: " + dateFormat.format(new Date()));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {

        T1T2_interest client = new T1T2_interest();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155", "10.80.25.155"};
        int port = 9042;
        client.connect(contact_points, port);
        List<String> listW1 = client.loadKeyword("/t1wordList");
        List<String> listW2 = client.loadKeyword("/t2wordList");

        String query1 = "select uid, t1, t2 from interest.uimge_values";

        client.loadData(query1, listW1, listW2);
        client.session.close();
        client.close();
    }
}



