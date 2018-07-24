package com.ifeng.model;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/*
* LSJ 2018/04/11
* people_groups导入uimge_manual_app2
* */

public class Impression_0418 {

    private Cluster cluster;

    private Session session;

    private Session getSession() {
        return session;
    }

    private void connect(String[] node, int port) {
        cluster = Cluster.builder().addContactPoints(node).withPort(port).build();
        cluster.getConfiguration().getQueryOptions().setFetchSize(50);
        this.session = cluster.connect("groups");
    }

    private void loadData(String query) {
        ResultSet resultSet = getSession().execute(query);
        int counter = 1;

        try {
            for (Row row : resultSet) {
                String uid = row.getString("dt");
                String str1 = uid.split("userid:")[1];

                session.execute("insert into ml.impression_uid_0418 " +
                                "(uid) " +
                                "VALUES (?) ",
                        str1
                );
                if (counter % 100000 == 0) {
                    System.out.println("impression_0418: " + counter);
                }
                counter++;
            }
            System.out.println("impression_0418: " + counter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        Impression_0418 client = new Impression_0418();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);

        String query2 = "select dt from ml.impression_0418_temp";

        client.loadData(query2);

        client.session.close();
        client.close();
    }
}
