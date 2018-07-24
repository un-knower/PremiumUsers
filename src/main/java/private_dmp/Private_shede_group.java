package private_dmp;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Private_shede_group {

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
                String uid = row.getString("uid");
                String loc = row.getString("loc");
                String city;
                if(loc != null && loc.split("_")[0].equals("中国")) {
                    city = loc.split("_")[1];
                }else if (loc != null) {
                    city = loc.split("_")[0];
                }else {
                    city = loc;
                }

                session.execute("insert into ml.private_shede_loc_temp " +
                                "(uid, " +
                                "city) " +
                                "VALUES (?,?) ",
                        uid,
                        city
                );

                if (counter % 100000 == 0) {
                    System.out.println("ml.private_shede_loc_temp: " + counter);
                }
                counter++;
            }
            System.out.println("ml.private_shede_loc_temp: " + counter);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {

        Private_shede_group client = new Private_shede_group();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);
        String query1 = "select uid, loc from uimg.private_shede_groups";

        client.loadData(query1);
        client.session.close();
        client.close();
    }
}




