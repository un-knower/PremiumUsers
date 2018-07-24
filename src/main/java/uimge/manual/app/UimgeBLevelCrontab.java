package uimge.manual.app;

import com.datastax.driver.core.*;
import uimge.util.DoRandom;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UimgeBLevelCrontab {
    private Cluster cluster;
    private Session session;
    private static final String INSERT_BLEVEL = "insert into groups.uimge_blevel" +
            "(uid,b101,b102,b103,b104,b105,b106," +
            "b201,b202," +
            "b301,b302,b303,b304,b305," +
            "b401,b402,b403,b404,b405,b406,b407," +
            "b501,b502,b503,b504) " +
            "VALUES (" +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?);";

    private Session getSession() {
        return session;
    }

    private void connect(String[] node, int port) {
        cluster = Cluster.builder().addContactPoints(node).withPort(port).build();
        cluster.getConfiguration().getQueryOptions().setFetchSize(50);
        this.session = cluster.connect("groups");
    }

    private String[] k1 = {"101","102","103","104","105","106"};
    private double[] v1 = {0.071281176, 0.116062779, 0.172004494, 0.191486539, 0.307663287, 0.113749563};
    private String[] k2 = {"201","202"};
    private double[] v2 = {0.61336852,0.400583575};
    private String[] k3 = {"301","302","303","304","305"};
    private double[] v3 = {0.419896396,0.151365032,0.154666505,0.130687204,0.139352604};
    private String[] k4 = {"401","402","403","404","405","406","407"};
    private double[] v4 = {0.12617171,0.153260836,0.240236013,0.132947721,0.092800194,0.079213506,0.161190915};
    private String[] k5 = {"501","502","503","504"};
    private double[] v5 = {0.268386684,0.295621499,0.188473555,0.258317799};
    private String[] k6 = {"601", "602", "603", "604", "605", "606", "607", "608", "609"};
    private double[] v6 = {0.012459592, 0.022563553, 0.003036001, 0.012556647, 0.006205595, 0.019095393, 0.00800201, 0.020875565, 0.001374366};
    private String[] k7 = {"701", "702", "703", "704", "705", "706", "707"};
    private double[] v7 = {0.013280205, 0.012619681, 0.00485085, 0.01030754, 0.004928205, 0.00522245, 0.006614849};
    private String[] k8 = {"801", "802", "803", "804", "805", "806"};
    private double[] v8 = {0.006666426, 0.009785375, 0.047125219, 0.016808297, 0.037363793, 0.056445649};
    private String[] k9 = {"901", "902", "903", "904", "905", "906", "907"};
    private double[] v9 = {0.002182931, 0.000624762, 0.026921568, 0.001952623, 0.011280139, 0.003467835, 0.004373981};
    private String[] k10 = {"1001", "1002", "1003", "1004", "1005", "1006", "1007"};
    private double[] v10 = {0.019680464, 0.004346062, 0.020377676, 0.059806052, 0.032260718, 0.012074887, 0.022156525};
    private String[] k11 = {"1101", "1102", "1103", "1104", "1105", "1106", "1107", "1108"};
    private double[] v11 = {0.0113497, 0.013836993, 0.005589908, 0.008653396, 0.013752892, 0.003767345, 0.011903048, 0.014103872};
    private String[] k12 = {"1201", "1202", "1203", "1204", "1205", "1206"};
    private double[] v12 = {0.000385368, 0.010524173, 0.008411469, 0.015324312, 0.007263405, 0.013715748};
    private String[] k13 = {"1301", "1302", "1303", "1304", "1305", "1306", "1307", "1308", "1309"};
    private double[] v13 = {0.014866323, 0.037738669, 0.015048553, 0.002800883, 0.033816873, 0.110039674, 0.010738963, 0.021591162, 0.001937551};
    private String[] k14 = {"1401", "1402", "1403", "1405", "1406", "1407", "1408"};
    private double[] v14 = {0.09064917, 0.024834485, 0.022892781, 0.018822767, 0.003333229, 0.0541362, 0.054856499};
    private String[] k15 = {"1501", "1502", "1503", "1504"};
    private double[] v15 = {0.039582064, 0.014691751, 0.034147957, 0.026023394};
    private String[] k16 = {"1601", "1602", "1603", "1604", "1605", "1606", "1607"};
    private double[] v16 = {0.006896256, 0.024143661, 0.007196232, 0.001279371, 0.000933325, 0.027918437, 0.015350347};
    private String[] k17 = {"1701", "1702", "1703"};
    private double[] v17 = {0.030911906, 0.028811968, 0.008453335};
    private String[] k18 = {"1801", "1802", "1803", "1804"};
    private double[] v18 = {0.040492569, 0.0510159, 0.040602677, 0.060127776};


    private void loadData(String query) {
        ResultSet resultSet = getSession().execute(query);
        PreparedStatement prepareStatement = session.prepare(INSERT_BLEVEL);
        int counter = 0;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        try {
            for (Row row : resultSet) {
                String uid = row.getString("uid");
                String cateid = row.getString("category_id");
                DoRandom doRandom = new DoRandom();
                int[] t1 = doRandom.dice(k1, v1);
                int[] t2 = doRandom.dice(k2, v2);
                int[] t3 = doRandom.dice(k3, v3);
                int[] t4 = doRandom.dice(k4, v4);
                int[] t5 = doRandom.dice(k5, v5);
                int[] t6 = doRandom.diceOnce(k6, v6);
                int[] t7 = doRandom.diceOnce(k7, v7);
                int[] t8 = doRandom.diceOnce(k8, v8);
                int[] t9 = doRandom.diceOnce(k9, v9);
                int[] t10 = doRandom.diceOnce(k10, v10);
                int[] t11 = doRandom.diceOnce(k11, v11);
                int[] t12 = doRandom.diceOnce(k12, v12);
                int[] t13 = doRandom.diceOnce(k13, v13);
                int[] t14 = doRandom.diceOnce(k14, v14);
                int[] t15 = doRandom.diceOnce(k15, v15);
                int[] t16 = doRandom.diceOnce(k16, v16);
                int[] t17 = doRandom.diceOnce(k17, v17);
                int[] t18 = doRandom.diceOnce(k18, v18);

                BoundStatement bindStatement = new BoundStatement(prepareStatement)
                        .bind(uid,t1[0],t1[1],t1[2],t1[3],t1[4],t1[5],t2[0],t2[1],
                                t3[0],t3[1],t3[2],t3[3],t3[4],
                                t4[0],t4[1],t4[2],t4[3],t4[4],t4[5],t4[6],
                                t5[0],t5[1],t5[2],t5[3]);
                session.execute(bindStatement);
//                session.execute("insert into groups.uimge_blevel" +
//                                "(uid,b101,b102,b103,b104,b105,b106," +
//                                "b201,b202," +
//                                "b301,b302,b303,b304,b305," +
//                                "b401,b402,b403,b404,b405,b406,b407," +
//                                "b501,b502,b503,b504) " +
//                                "VALUES (" +
//                                "?,?,?,?,?,?,?,?,?,?," +
//                                "?,?,?,?,?,?,?,?,?,?," +
//                                "?,?,?,?,?) ",
//                        uid,
//                        t1[0],t1[1],t1[2],t1[3],t1[4],t1[5],
//                        t2[0],t2[1],
//                        t3[0],t3[1],t3[2],t3[3],t3[4],
//                        t4[0],t4[1],t4[2],t4[3],t4[4],t4[5],t4[6],
//                        t5[0],t5[1],t5[2],t5[3]
//                );

                switch (cateid) {
                    case "c600":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b601,b602,b603,b604,b605,b606,b607,b608,b609) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?,?) ",
                                uid,
                                t6[0], t6[1], t6[2], t6[3], t6[4], t6[5], t6[6], t6[7], t6[8]
                        );
                        break;
                    case "c700":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b701,b702,b703,b704,b705,b706,b707) " +
                                        "VALUES (?,?,?,?,?,?,?,?) ",
                                uid,
                                t7[0], t7[1], t7[2], t7[3], t7[4], t7[5], t7[6]
                        );
                        break;
                    case "c800":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b801,b802,b803,b804,b805,b806) " +
                                        "VALUES (?,?,?,?,?,?,?) ",
                                uid,
                                t8[0], t8[1], t8[2], t8[3], t8[4], t8[5]
                        );
                        break;
                    case "c900":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b901,b902,b903,b904,b905,b906,b907) " +
                                        "VALUES (?,?,?,?,?,?,?,?) ",
                                uid,
                                t9[0], t9[1], t9[2], t9[3], t9[4], t9[5], t9[6]
                        );
                        break;
                    case "c1000":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b1001,b1002,b1003,b1004,b1005,b1006,b1007) " +
                                        "VALUES (?,?,?,?,?,?,?,?) ",
                                uid,
                                t10[0], t10[1], t10[2], t10[3], t10[4], t10[5], t10[6]
                        );
                        break;
                    case "c1100":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b1101,b1102,b1103,b1104,b1105,b1106,b1107,b1108) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?) ",
                                uid,
                                t11[0], t11[1], t11[2], t11[3], t11[4], t11[5], t11[6], t11[7]
                        );
                        break;
                    case "c1200":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b1201,b1202,b1203,b1204,b1205,b1206) " +
                                        "VALUES (?,?,?,?,?,?,?) ",
                                uid,
                                t12[0], t12[1], t12[2], t12[3], t12[4], t12[5]
                        );
                        break;
                    case "c1300":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b1301,b1302,b1303,b1304,b1305,b1306,b1307,b1308,b1309) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?,?) ",
                                uid,
                                t13[0], t13[1], t13[2], t13[3], t13[4], t13[5], t13[6], t13[7], t13[8]
                        );
                        break;
                    case "c1400":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b1401,b1402,b1403,b1405,b1406,b1407,b1408) " +
                                        "VALUES (?,?,?,?,?,?,?,?) ",
                                uid,
                                t14[0], t14[1], t14[2], t14[3], t14[4], t14[5], t14[6]
                        );
                        break;
                    case "c1500":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b1501,b1502,b1503,b1504) " +
                                        "VALUES (?,?,?,?,?) ",
                                uid,
                                t15[0], t15[1], t15[2], t15[3]
                        );
                        break;
                    case "c1600":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b1601,b1602,b1603,b1604,b1605,b1606,b1607) " +
                                        "VALUES (?,?,?,?,?,?,?,?) ",
                                uid,
                                t16[0], t16[1], t16[2], t16[3], t16[4], t16[5], t16[6]
                        );
                        break;
                    case "c1700":
                        session.execute("insert into groups.uimge_blevel " +
                                        "(uid, b1701,b1702,b1703) " +
                                        "VALUES (?,?,?,?) ",
                                uid,
                                t17[0], t17[1], t17[2]
                        );
                        break;
                    case "c1800":
                        session.execute("insert into uimg.uimge_manual_app2 " +
                                        "(uid, b1801,b1802,b1803,b1804) " +
                                        "VALUES (?,?,?,?,?) ",
                                uid,
                                t18[0], t18[1], t18[2], t18[3]
                        );
                        break;
                    default:
                        break;
                }
                if (counter % 1000000 == 0) {
                    System.out.println("uimge_Blevel: "+counter +
                            " Time: " + dateFormat.format(new Date()));
                }
                counter++;
            }
            System.out.println("uimge_Blevel: " + counter +
                    " Time: " + dateFormat.format(new Date()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        UimgeBLevelCrontab client = new UimgeBLevelCrontab();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.23.155", "10.80.24.155", "10.80.25.155"};
        int port = 9042;
        client.connect(contact_points, port);

        String query = "select * from groups.people_groups where label = '2' allow filtering";

        client.loadData(query);

        client.session.close();
        client.close();
    }
}

