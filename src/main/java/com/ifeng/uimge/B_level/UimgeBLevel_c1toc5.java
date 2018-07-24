package com.ifeng.uimge.B_level;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class UimgeBLevel_c1toc5 {
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

    private String[] k1 = {"101","102","103","104","105","106"};
    private double[] v1 = {0.171281176, 0.126062779, 0.152004494, 0.161486539, 0.257663287, 0.133749563};
    private String[] k2 = {"201","202"};
    private double[] v2 = {0.60336852,0.400583575};
    private String[] k3 = {"301","302","303","304","305"};
    private double[] v3 = {0.239896396,0.171365032,0.174666505,0.150687204,0.259352604 };
    private String[] k4 = {"401","402","403","404","405","406","407"};
    private double[] v4 = {0.24617171,0.123260836,0.140236013,0.122947721,0.092800194,0.109213506,0.161190915 };
    private String[] k5 = {"501","502","503","504"};
    private double[] v5 = {0.168386684,0.205621499,0.338473555,0.298317799  };

    private void loadData(String query) {
        ResultSet resultSet = getSession().execute(query);
        int counter = 0;


        try {
            for (Row row : resultSet) {
                String uid = row.getString("uid");
                DoRandom doRandom = new DoRandom();
                int[] t1 = doRandom.dice(k1, v1);
                int[] t2 = doRandom.dice(k2, v2);
                int[] t3 = doRandom.dice(k3, v3);
                int[] t4 = doRandom.dice(k4, v4);
                int[] t5 = doRandom.dice(k5, v5);

                session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid," +
                                    "b101,b102,b103,b104,b105,b106," +
                                    "b201,b202," +
                                    "b301,b302,b303,b304,b305," +
                                    "b401,b402,b403,b404,b405,b406,b407," +
                                    "b501,b502,b503,b504) " +
                                    "VALUES (?," +
                                    "?,?,?,?,?,?," +
                                    "?,?," +
                                    "?,?,?,?,?," +
                                    "?,?,?,?,?,?,?," +
                                    "?,?,?,?) ",
                            uid,
                            t1[0], t1[1], t1[2], t1[3], t1[4], t1[5],
                            t2[0], t2[1],
                            t3[0], t3[1], t3[2], t3[3], t3[4],
                            t4[0], t4[1], t4[2], t4[3], t4[4], t4[5], t4[6],
                            t5[0], t5[1], t5[2], t5[3]
                );

                if (counter % 100000 == 0) {
                    System.out.println("manual_Blevel_c1toc5: " + counter);
                }
                counter++;
            }
            System.out.println("manual_Blevel_c1toc5: " + counter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        UimgeBLevel_c1toc5 client = new UimgeBLevel_c1toc5();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);

        String query2 = "select uid from uimg.uimge_manual_app2";

        client.loadData(query2);

        client.session.close();
        client.close();
    }
}
