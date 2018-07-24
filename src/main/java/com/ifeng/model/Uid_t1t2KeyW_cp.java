package com.ifeng.model;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class Uid_t1t2KeyW_cp {
    private Cluster cluster;

    private Session session;

    private Session getSession() {
        return session;
    }

    private void connect(String[] node, int port) {
        cluster = Cluster.builder().addContactPoints(node).withPort(port).build();
        cluster.getConfiguration().getQueryOptions().setFetchSize(5000);
        this.session = cluster.connect("groups");
    }

    private void createSchema(Session session) {
        session.execute(
                "CREATE TABLE IF NOT EXISTS uid_index_keywords (" +
                        "uid text PRIMARY KEY," +
                        "keywords text," +
                        "row_num int" +
                        ");"
        );
    }

    private void loadData(String query) {
        ResultSet resultSet = getSession().execute(query);
        int counter = 1;

        try {
            for (Row row : resultSet) {
                String uid = row.getString("uid");
                String keywords = row.getString("keywords");

                session.execute("insert into uid_index_keywords " +
                                "(uid, " +
                                "keywords, " +
                                "row_num) " +
                                "VALUES (?,?,?) ",
                        uid,
                        keywords,
                        counter
                );
                if (counter % 100000 == 0) {
                    System.out.println("vehicle_libsvm_t1t2: " + counter);
                }
                counter++;
            }
            System.out.println("vehicle_libsvm_t1t2: " + counter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        Uid_t1t2KeyW_cp client = new Uid_t1t2KeyW_cp();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);
        client.createSchema(client.session);
        String query1 = "select uid,keywords from uid_t1t2keywords";

        client.loadData(query1);
        client.session.close();
        client.close();
    }
}
