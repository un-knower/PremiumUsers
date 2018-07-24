package com.ifeng.mysql;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ActionDataMySQL {

    private Cluster cluster;
    private Session session;
    private Session getSession() {
        return session;
    }
    private static final String INSERT_ACTION_DATA = "insert into groups.action_data " +
            "(id,imei,idfa,creative_id,ip,imp_id) values (?,?,?,?,?,?);";

    private void connectCass(String[] node, int port) {
        cluster = Cluster.builder().addContactPoints(node).withPort(port).build();
        cluster.getConfiguration().getQueryOptions().setFetchSize(50);
        this.session = cluster.connect("uimg");
    }

    private void loadMySQL() {

        String conn_str = "jdbc:mysql://10.90.9.110:3306/BI?" +
                "user=readonly&password=readonly.com&" +
                "useUnicode=true&characterEncoding=UTF8";

        PreparedStatement prepareStatement = session.prepare(INSERT_ACTION_DATA);
        int counter = 0;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        try(Connection conn = DriverManager.getConnection(conn_str)) {
            Class.forName("com.mysql.cj.jdbc.Driver");
            try(Statement stmt = conn.createStatement()) {
                String sql = "select * from effect_data";
                ResultSet res = stmt.executeQuery(sql);
                if (res != null) {
                    while (res.next()) {
                        BoundStatement bindStatement = new BoundStatement(prepareStatement)
                                .bind(res.getString("id"),
                                        res.getString("imei"),
                                        res.getString("idfa"),
                                        res.getString("creative_id"),
                                        res.getString("ip"),
                                        res.getString("imp_id"));
                        session.execute(bindStatement);
                    }
                }
            }
            System.out.println("insert action data: " + counter +
                    " Time: " + dateFormat.format(new Date()));
        } catch (SQLException e) {
            System.out.println("MySQL error");
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {

        ActionDataMySQL client = new ActionDataMySQL();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.23.155", "10.80.24.155", "10.80.25.155"};
        int port = 9042;
        client.connectCass(contact_points, port);
        client.loadMySQL();
        client.session.close();
        client.close();
    }
}
