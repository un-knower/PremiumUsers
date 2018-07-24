package com.ifeng.uimge.B_level;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class UimgeBLevel {
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

    String[] k6 = {"601","602","603","604","605","606","607","608","609"};
    double[] v6 = {0.012459592,0.022563553,0.003036001,0.012556647,0.006205595 ,0.019095393,0.00800201,0.020875565,0.001374366 };
    String[] k7 = {"701","702","703","704","705","706","707"};
    double[] v7 = {0.013280205 ,0.012619681,0.00485085 ,0.01030754 ,0.004928205,0.00522245 ,0.006614849 };
    String[] k8 = {"801","802","803","804","805","806"};
    double[] v8 = {0.006666426,0.009785375,0.047125219,0.016808297,0.037363793,0.056445649};
    String[] k9 = {"901","902","903","904","905","906","907"};
    double[] v9 = {0.002182931,0.000624762,0.026921568,0.001952623,0.011280139,0.003467835,0.004373981};
    String[] k10 = {"1001","1002","1003","1004","1005","1006","1007"};
    double[] v10 = {0.019680464,0.004346062,0.020377676,0.059806052,0.032260718,0.012074887,0.022156525 };
    String[] k11 = {"1101","1102","1103","1104","1105","1106","1107","1108"};
    double[] v11 = {0.0113497,0.013836993,0.005589908,0.008653396,0.013752892,0.003767345,0.011903048,0.014103872 };
    String[] k12 = {"1201","1202","1203","1204","1205","1206"};
    double[] v12 = {0.000385368,0.010524173,0.008411469,0.015324312,0.007263405,0.013715748};
    String[] k13 = {"1301","1302","1303","1304","1305","1306","1307","1308","1309"};
    double[] v13 = {0.014866323,0.037738669,0.015048553,0.002800883,0.033816873,0.110039674,0.010738963,0.021591162,0.001937551 };
    String[] k14 = {"1401","1402","1403","1405","1406","1407","1408"};
    double[] v14 = {0.09064917,0.024834485,0.022892781,0.018822767,0.003333229,0.0541362,0.054856499 };
    String[] k15 = {"1501","1502","1503","1504"};
    double[] v15 = {0.039582064,0.014691751,0.034147957,0.026023394};
    String[] k16 = {"1601","1602","1603","1604","1605","1606","1607"};
    double[] v16 = {0.006896256,0.024143661,0.007196232,0.001279371,0.000933325,0.027918437,0.015350347};
    String[] k17 = {"1701","1702","1703"};
    double[] v17 = {0.030911906,0.028811968,0.008453335 };
    String[] k18 = {"1801","1802","1803","1804"};
    double[] v18 = {0.040492569,0.0510159,0.040602677,0.060127776};


    private void loadData(String query) {
        ResultSet resultSet = getSession().execute(query);
        int counter = 0;


        try {
            for (Row row : resultSet) {
                String uid = row.getString("uid");
                DoRandom doRandom = new DoRandom();
                int[] t6 = doRandom.dice(k6, v6);
                int[] t7 = doRandom.dice(k7, v7);
                int[] t8 = doRandom.dice(k8, v8);
                int[] t9 = doRandom.dice(k9, v9);
                int[] t10 = doRandom.dice(k10, v10);
                int[] t11 = doRandom.dice(k11, v11);
                int[] t12 = doRandom.dice(k12, v12);
                int[] t13 = doRandom.dice(k13, v13);
                int[] t14 = doRandom.dice(k14, v14);
                int[] t15 = doRandom.dice(k15, v15);
                int[] t16 = doRandom.dice(k16, v16);
                int[] t17 = doRandom.dice(k17, v17);
                int[] t18 = doRandom.dice(k18, v18);

                if(row.getInt("c600") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b601,b602,b603,b604,b605,b606,b607,b608,b609) " +
                                    "VALUES (?,?,?,?,?,?,?,?,?,?) ",
                            uid,
                            t6[0],t6[1],t6[2],t6[3],t6[4],t6[5],t6[6],t6[7],t6[8]
                    );
                }
                if(row.getInt("c700") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b701,b702,b703,b704,b705,b706,b707) " +
                                    "VALUES (?,?,?,?,?,?,?,?) ",
                            uid,
                            t7[0],t7[1],t7[2],t7[3],t7[4],t7[5],t7[6]
                    );
                }
                if(row.getInt("c800") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b801,b802,b803,b804,b805,b806) " +
                                    "VALUES (?,?,?,?,?,?,?) ",
                            uid,
                            t8[0],t8[1],t8[2],t8[3],t8[4],t8[5]
                    );
                }
                if(row.getInt("c900") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b901,b902,b903,b904,b905,b906,b907) " +
                                    "VALUES (?,?,?,?,?,?,?,?) ",
                            uid,
                            t9[0],t9[1],t9[2],t9[3],t9[4],t9[5],t9[6]
                    );
                }
                if(row.getInt("c1000") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b1001,b1002,b1003,b1004,b1005,b1006,b1007) " +
                                    "VALUES (?,?,?,?,?,?,?,?) ",
                            uid,
                            t10[0],t10[1],t10[2],t10[3],t10[4],t10[5],t10[6]
                    );
                }
                if(row.getInt("c1100") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b1101,b1102,b1103,b1104,b1105,b1106,b1107,b1108) " +
                                    "VALUES (?,?,?,?,?,?,?,?,?) ",
                            uid,
                            t11[0],t11[1],t11[2],t11[3],t11[4],t11[5],t11[6],t11[7]
                    );
                }
                if(row.getInt("c1200") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b1201,b1202,b1203,b1204,b1205,b1206) " +
                                    "VALUES (?,?,?,?,?,?,?) ",
                            uid,
                            t12[0],t12[1],t12[2],t12[3],t12[4],t12[5]
                    );
                }
                if(row.getInt("c1300") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b1301,b1302,b1303,b1304,b1305,b1306,b1307,b1308,b1309) " +
                                    "VALUES (?,?,?,?,?,?,?,?,?,?) ",
                            uid,
                            t13[0],t13[1],t13[2],t13[3],t13[4],t13[5],t13[6],t13[7],t13[8]
                    );
                }
                if(row.getInt("c1400") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b1401,b1402,b1403,b1405,b1406,b1407,b1408) " +
                                    "VALUES (?,?,?,?,?,?,?,?) ",
                            uid,
                            t14[0],t14[1],t14[2],t14[3],t14[4],t14[5],t14[6]
                    );
                }
                if(row.getInt("c1500") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b1501,b1502,b1503,b1504) " +
                                    "VALUES (?,?,?,?,?) ",
                            uid,
                            t15[0],t15[1],t15[2],t15[3]
                    );
                }
                if(row.getInt("c1600") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b1601,b1602,b1603,b1604,b1605,b1606,b1607) " +
                                    "VALUES (?,?,?,?,?,?,?,?) ",
                            uid,
                            t16[0],t16[1],t16[2],t16[3],t16[4],t16[5],t16[6]
                    );
                }
                if(row.getInt("c1700") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b1701,b1702,b1703) " +
                                    "VALUES (?,?,?,?) ",
                            uid,
                            t17[0],t17[1],t17[2]
                    );
                }
                if(row.getInt("c1800") == 1) {
                    session.execute("insert into uimg.uimge_manual_app2 " +
                                    "(uid, b1801,b1802,b1803,b1804) " +
                                    "VALUES (?,?,?,?,?) ",
                            uid,
                            t18[0],t18[1],t18[2],t18[3]
                    );
                }
                if (counter % 50000 == 0) {
                    System.out.println("manual_Blevel: " + counter);
                }
                counter++;
            }
            System.out.println("manual_Blevel: " + counter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        UimgeBLevel client = new UimgeBLevel();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);

        String query2 = "select uid,c600,c700,c800,c900,c1000,c1100,c1200,c1300,c1400,c1500,c1600,c1700,c1800 from uimg.uimge_manual_app2";

        client.loadData(query2);

        client.session.close();
        client.close();
    }
}
