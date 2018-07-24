package uimge.manual.app;

import com.datastax.driver.core.*;
import uimge.util.DoRandom;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UimgeBLevelAllCate {
    private Cluster cluster;
    private Session session;
    private static final String INSERT_RND_LEVEL = "insert into groups.uimge_blevel_allcate " +
            "(uid, " +
            "b101, " +
            "b102, " +
            "b103, " +
            "b104, " +
            "b105, " +
            "b106, " +
            "b201, " +
            "b202, " +
            "b301, " +
            "b302, " +
            "b303, " +
            "b304, " +
            "b305, " +
            "b401, " +
            "b402, " +
            "b403, " +
            "b404, " +
            "b405, " +
            "b406, " +
            "b407, " +
            "b501, " +
            "b502, " +
            "b503, " +
            "b504, " +
            "b601, " +
            "b602, " +
            "b603, " +
            "b604, " +
            "b605, " +
            "b606, " +
            "b607, " +
            "b608, " +
            "b609, " +
            "b701, " +
            "b702, " +
            "b703, " +
            "b704, " +
            "b705, " +
            "b706, " +
            "b707, " +
            "b801, " +
            "b802, " +
            "b803, " +
            "b804, " +
            "b805, " +
            "b806, " +
            "b901, " +
            "b902, " +
            "b903, " +
            "b904, " +
            "b905, " +
            "b906, " +
            "b907, " +
            "b1001, " +
            "b1002, " +
            "b1003, " +
            "b1004, " +
            "b1005, " +
            "b1006, " +
            "b1007, " +
            "b1101, " +
            "b1102, " +
            "b1103, " +
            "b1104, " +
            "b1105, " +
            "b1106, " +
            "b1107, " +
            "b1108, " +
            "b1201, " +
            "b1202, " +
            "b1203, " +
            "b1204, " +
            "b1205, " +
            "b1206, " +
            "b1301, " +
            "b1302, " +
            "b1303, " +
            "b1304, " +
            "b1305, " +
            "b1306, " +
            "b1307, " +
            "b1308, " +
            "b1309, " +
            "b1401, " +
            "b1402, " +
            "b1403, " +
            "b1405, " +
            "b1406, " +
            "b1407, " +
            "b1408, " +
            "b1501, " +
            "b1502, " +
            "b1503, " +
            "b1504, " +
            "b1601, " +
            "b1602, " +
            "b1603, " +
            "b1604, " +
            "b1605, " +
            "b1606, " +
            "b1607, " +
            "b1701, " +
            "b1702, " +
            "b1703, " +
            "b1801, " +
            "b1802, " +
            "b1803, " +
            "b1804) " +
            "VALUES (?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?);";

    private Session getSession() {
        return session;
    }

    private void connect(String[] node, int port) {
        cluster = Cluster.builder().addContactPoints(node).withPort(port).build();
        cluster.getConfiguration().getQueryOptions().setFetchSize(20);
        this.session = cluster.connect("uimg");
    }

    private String[] k1 = {"101","102","103","104","105","106"};
    private double[] v1 = {0.091281176, 0.126062779, 0.192004494, 0.201486539, 0.247663287, 0.133749563};
    private String[] k2 = {"201","202"};
    private double[] v2 = {0.65336852,0.350583575};
    private String[] k3 = {"301","302","303","304","305"};
    private double[] v3 = {0.389896396,0.191365032,0.114666505,0.130687204,0.16352604 };
    private String[] k4 = {"401","402","403","404","405","406","407"};
    private double[] v4 = {0.13617171,0.163260836,0.190236013,0.142947721,0.092800194,0.079213506,0.131190915 };
    private String[] k5 = {"501","502","503","504"};
    private double[] v5 = {0.188386684,0.205621499,0.308473555,0.248317799  };
    private String[] k6 = {"601", "602", "603", "604", "605", "606", "607", "608", "609"};
    private double[] v6 = {0.12459592, 0.22563553, 0.13036001, 0.12556647, 0.06205595, 0.19095393, 0.1100201, 0.10875565, 0.1374366};
    private String[] k7 = {"701", "702", "703", "704", "705", "706", "707"};
    private double[] v7 = {0.06280205, 0.12619681, 0.0485085, 0.1030754, 0.04928205, 0.0522245, 0.06614849};
    private String[] k8 = {"801", "802", "803", "804", "805", "806"};
    private double[] v8 = {0.06666426, 0.09785375, 0.047125219, 0.016808297, 0.037363793, 0.056445649};
    private String[] k9 = {"901", "902", "903", "904", "905", "906", "907"};
    private double[] v9 = {0.1182931, 0.124762, 0.26921568, 0.1952623, 0.11280139, 0.08467835, 0.06373981};
    private String[] k10 = {"1001", "1002", "1003", "1004", "1005", "1006", "1007"};
    private double[] v10 = {0.19680464, 0.06346062, 0.20377676, 0.29806052, 0.32260718, 0.12074887, 0.22156525};
    private String[] k11 = {"1101", "1102", "1103", "1104", "1105", "1106", "1107", "1108"};
    private double[] v11 = {0.113497, 0.13836993, 0.25589908, 0.08653396, 0.13752892, 0.13767345, 0.11903048, 0.12103872};
    private String[] k12 = {"1201", "1202", "1203", "1204", "1205", "1206"};
    private double[] v12 = {0.285368, 0.10524173, 0.08411469, 0.15324312, 0.07263405, 0.13715748};
    private String[] k13 = {"1301", "1302", "1303", "1304", "1305", "1306", "1307", "1308", "1309"};
    private double[] v13 = {0.07866323, 0.17738669, 0.11048553, 0.0800883, 0.13816873, 0.150039674, 0.10738963, 0.21591162, 0.0937551};
    private String[] k14 = {"1401", "1402", "1403", "1405", "1406", "1407", "1408"};
    private double[] v14 = {0.05064917, 0.024834485, 0.022892781, 0.018822767, 0.03333229, 0.0541362, 0.054856499};
    private String[] k15 = {"1501", "1502", "1503", "1504"};
    private double[] v15 = {0.39582064, 0.14691751, 0.34147957, 0.26023394};
    private String[] k16 = {"1601", "1602", "1603", "1604", "1605", "1606", "1607"};
    private double[] v16 = {0.06896256, 0.24143661, 0.17196232, 0.1279371, 0.0933325, 0.17918437, 0.15350347};
    private String[] k17 = {"1701", "1702", "1703"};
    private double[] v17 = {0.30911906, 0.28811968, 0.08453335};
    private String[] k18 = {"1801", "1802", "1803", "1804"};
    private double[] v18 = {0.040492569, 0.0510159, 0.040602677, 0.060127776};

    private void loadData(String query) {
        ResultSet resultSet = getSession().execute(query);
        PreparedStatement prepareStatement = session.prepare(INSERT_RND_LEVEL);
        int counter = 0;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        try {
            for (Row row : resultSet) {
                String uid = row.getString("uid");
                DoRandom doRandom = new DoRandom();
                int[] t1 = doRandom.dice(k1, v1);
                int[] t2 = doRandom.dice(k2, v2);
                int[] t3 = doRandom.dice(k3, v3);
                int[] t4 = doRandom.dice(k4, v4);
                int[] t5 = doRandom.dice(k5, v5);
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

                BoundStatement bindStatement = new BoundStatement(prepareStatement)
                        .bind(uid,t1[0], t1[1], t1[2], t1[3], t1[4], t1[5],
                                t2[0], t2[1],
                                t3[0], t3[1], t3[2], t3[3], t3[4],
                                t4[0], t4[1], t4[2], t4[3], t4[4], t4[5], t4[6],
                                t5[0], t5[1], t5[2], t5[3],
                                t6[0], t6[1], t6[2], t6[3], t6[4], t6[5], t6[6], t6[7], t6[8],
                                t7[0], t7[1], t7[2], t7[3], t7[4], t7[5], t7[6],
                                t8[0], t8[1], t8[2], t8[3], t8[4], t8[5],
                                t9[0], t9[1], t9[2], t9[3], t9[4], t9[5], t9[6],
                                t10[0], t10[1], t10[2], t10[3], t10[4], t10[5], t10[6],
                                t11[0], t11[1], t11[2], t11[3], t11[4], t11[5], t11[6], t11[7],
                                t12[0], t12[1], t12[2], t12[3], t12[4], t12[5],
                                t13[0], t13[1], t13[2], t13[3], t13[4], t13[5], t13[6], t13[7], t13[8],
                                t14[0], t14[1], t14[2], t14[3], t14[4], t14[5], t14[6],
                                t15[0], t15[1], t15[2], t15[3],
                                t16[0], t16[1], t16[2], t16[3], t16[4], t16[5], t16[6],
                                t17[0], t17[1], t17[2],
                                t18[0], t18[1], t18[2], t18[3]);
                session.execute(bindStatement);

                if (counter % 100000 == 0) {
                    System.out.println("uimge_blevel_allcate: "+counter +
                            " Time: " + dateFormat.format(new Date()));
                }
                counter++;
            }
            System.out.println("uimge_blevel_allcate: " + counter +
                    " Time: " + dateFormat.format(new Date()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        UimgeBLevelAllCate client = new UimgeBLevelAllCate();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.23.155", "10.80.24.155", "10.80.25.155"};
        int port = 9042;
        client.connect(contact_points, port);

        String query2 = "select * from uimg.uimge_uidlist";

        client.loadData(query2);

        client.session.close();
        client.close();
    }
}


