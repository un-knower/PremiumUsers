package com.ifeng.model;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class Non_game_users_libsvm {
    private Cluster cluster;

    private Session session;

    private Session getSession() {
        return session;
    }

    private void connect(String[] node, int port) {
        cluster = Cluster.builder().addContactPoints(node).withPort(port).build();
        cluster.getConfiguration().getQueryOptions().setFetchSize(50);
        this.session = cluster.connect("ml");
    }

    private void loadData(String query, String filePath) {
        ResultSet resultSet = getSession().execute(query);
        int counter = 1;

        try {
            FileWriter fw = new FileWriter(filePath, true);
            BufferedWriter bw = new BufferedWriter(fw);

            for (Row row : resultSet) {
                int id = row.getInt("id");
                String keyword = row.getString("keyword");

                StringBuilder stringBuilder = new StringBuilder(id);
                stringBuilder.append(keyword);
                bw.write(stringBuilder + "\n");
                if (counter % 10000 == 0) {
                    System.out.println("vehicle_libsvm_t1t2: " + counter);
                }
                counter++;
            }
            System.out.println("vehicle_libsvm_t1t2: " + counter);
            bw.close();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        Non_game_users_libsvm client = new Non_game_users_libsvm();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);
//        String query1 = "select uid,keyword from non_game_t1t2_libsvm";
        String query1 = "select id,keyword from non_game_t1t2_libsvm";
        String filePath1 = "D:\\Work\\data\\non_game_users_libsvm.txt";

        client.loadData(query1, filePath1);
        client.session.close();
        client.close();
    }
}
