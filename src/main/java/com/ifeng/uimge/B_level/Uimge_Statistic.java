package com.ifeng.uimge.B_level;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Uimge_Statistic {

    private DoRandom doRandom = new DoRandom();
    private String[] k1 = {"101", "102", "103", "104", "105", "106"};
    private double[] v1 = {0.171281176, 0.126062779, 0.152004494, 0.161486539, 0.257663287, 0.133749563};
    private String[] k2 = {"201", "202"};
    private double[] v2 = {0.60336852, 0.400583575};
    private String[] k3 = {"301", "302", "303", "304", "305"};
    private double[] v3 = {0.239896396, 0.171365032, 0.174666505, 0.150687204, 0.259352604};
    private String[] k4 = {"401", "402", "403", "404", "405", "406", "407"};
    private double[] v4 = {0.24617171, 0.123260836, 0.140236013, 0.122947721, 0.092800194, 0.109213506, 0.161190915};
    private String[] k5 = {"501", "502", "503", "504"};
    private double[] v5 = {0.168386684, 0.205621499, 0.338473555, 0.298317799};
    private String[] k6 = {"601", "602", "603", "604", "605", "606", "607", "608", "609"};
    private double[] v6 = {0.1173853, 0.21257754, 0.02860301, 0.11829967, 0.05846465, 0.17990303, 0.07538917, 0.19667453, 0.01294828};
    private String[] k7 = {"701", "702", "703", "704", "705", "706", "707"};
    private double[] v7 = {0.15403988, 0.14637832, 0.05626602, 0.11955932, 0.05716328, 0.06057629, 0.07672701};
    private String[] k8 = {"801", "802", "803", "804", "805", "806"};
    private double[] v8 = {0.08194486, 0.12028353, 0.57927139, 0.2066105, 0.45928225, 0.69383973};
    private String[] k9 = {"901", "902", "903", "904", "905", "906", "907"};
    private double[] v9 = {0.04315954, 0.05033551, 0.36345379, 0.10466416, 0.15262207, 0.07282628, 0.06629579};
    private String[] k10 = {"1001", "1002", "1003", "1004", "1005", "1006", "1007"};
    private double[] v10 = {0.10026236, 0.02214106, 0.10381432, 0.30468267, 0.16435263, 0.06151566, 0.11287669};
    private String[] k11 = {"1101", "1102", "1103", "1104", "1105", "1106", "1107", "1108"};
    private double[] v11 = {0.1247591, 0.17736067, 0.10021621, 0.12860668, 0.4008112, 0.04709181, 0.13945512, 0.20557098};
    private String[] k12 = {"1201", "1202", "1203", "1204", "1205", "1206"};
    private double[] v12 = {0.00579127, 0.15815614, 0.12640666, 0.23029212, 0.10915368, 0.2061188};
    private String[] k13 = {"1301", "1302", "1303", "1304", "1305", "1306", "1307", "1308", "1309"};
    private double[] v13 = {0.04606937, 0.19985227, 0.1287358, 0.08404095, 0.06907464, 0.35591797, 0.18001741, 0.25716182, 0.16976671};
    private String[] k14 = {"1401", "1402", "1403", "1405", "1406", "1407", "1408"};
    private double[] v14 = {0.70100189, 0.19204832, 0.17703287, 0.14555892, 0.0257763, 0.41864232, 0.42421248};
    private String[] k15 = {"1501", "1502", "1503", "1504"};
    private double[] v15 = {0.30995083, 0.11504505, 0.26739858, 0.20377847};
    private String[] k16 = {"1601", "1602", "1603", "1604", "1605", "1606", "1607"};
    private double[] v16 = {0.07252001, 0.25389118, 0.07567451, 0.01345368, 0.0098147, 0.29358617, 0.16142198};
    private String[] k17 = {"1701", "1702", "1703"};
    private double[] v17 = {0.27934126, 0.26036477, 0.07639015};
    private String[] k18 = {"1801", "1802", "1803", "1804"};
    private double[] v18 = {0.1603078, 0.20196907, 0.16074371, 0.23804248
    };

    private Cluster cluster;
    private Session session;

    private Session getSession() {
        return session;
    }

    private void connect(String[] node, int port) {
        cluster = Cluster.builder().addContactPoints(node).withPort(port).build();
        cluster.getConfiguration().getQueryOptions().setFetchSize(50);
        this.session = cluster.connect("uimg");
    }

    private void createSchema() {

        session.execute(
                "CREATE TABLE IF NOT EXISTS uimg.uimge_statistic (" +
                        "uid text PRIMARY KEY," +
                        "b101 int, " +
                        "b102 int, " +
                        "b103 int, " +
                        "b104 int, " +
                        "b105 int, " +
                        "b106 int, " +
                        "b201 int, " +
                        "b202 int, " +
                        "b301 int, " +
                        "b302 int, " +
                        "b303 int, " +
                        "b304 int, " +
                        "b305 int, " +
                        "b401 int, " +
                        "b402 int, " +
                        "b403 int, " +
                        "b404 int, " +
                        "b405 int, " +
                        "b406 int, " +
                        "b407 int, " +
                        "b501 int, " +
                        "b502 int, " +
                        "b503 int, " +
                        "b504 int, " +
                        "b601 int, " +
                        "b602 int, " +
                        "b603 int, " +
                        "b604 int, " +
                        "b605 int, " +
                        "b606 int, " +
                        "b607 int, " +
                        "b608 int, " +
                        "b609 int, " +
                        "b701 int, " +
                        "b702 int, " +
                        "b703 int, " +
                        "b704 int, " +
                        "b705 int, " +
                        "b706 int, " +
                        "b707 int, " +
                        "b801 int, " +
                        "b802 int, " +
                        "b803 int, " +
                        "b804 int, " +
                        "b805 int, " +
                        "b806 int, " +
                        "b901 int, " +
                        "b902 int, " +
                        "b903 int, " +
                        "b904 int, " +
                        "b905 int, " +
                        "b906 int, " +
                        "b907 int, " +
                        "b1001 int, " +
                        "b1002 int, " +
                        "b1003 int, " +
                        "b1004 int, " +
                        "b1005 int, " +
                        "b1006 int, " +
                        "b1007 int, " +
                        "b1101 int, " +
                        "b1102 int, " +
                        "b1103 int, " +
                        "b1104 int, " +
                        "b1105 int, " +
                        "b1106 int, " +
                        "b1107 int, " +
                        "b1108 int, " +
                        "b1201 int, " +
                        "b1202 int, " +
                        "b1203 int, " +
                        "b1204 int, " +
                        "b1205 int, " +
                        "b1206 int, " +
                        "b1301 int, " +
                        "b1302 int, " +
                        "b1303 int, " +
                        "b1304 int, " +
                        "b1305 int, " +
                        "b1306 int, " +
                        "b1307 int, " +
                        "b1308 int, " +
                        "b1309 int, " +
                        "b1401 int, " +
                        "b1402 int, " +
                        "b1403 int, " +
                        "b1405 int, " +
                        "b1406 int, " +
                        "b1407 int, " +
                        "b1408 int, " +
                        "b1501 int, " +
                        "b1502 int, " +
                        "b1503 int, " +
                        "b1504 int, " +
                        "b1601 int, " +
                        "b1602 int, " +
                        "b1603 int, " +
                        "b1604 int, " +
                        "b1605 int, " +
                        "b1606 int, " +
                        "b1607 int, " +
                        "b1701 int, " +
                        "b1702 int, " +
                        "b1703 int, " +
                        "b1801 int, " +
                        "b1802 int, " +
                        "b1803 int, " +
                        "b1804 int, " +
                        "dt timestamp " +
                        ");"
        );
    }

    private void loadData(String query) {
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

        ResultSet resultSet = getSession().execute(query);
        int counter = 0;
        try {
            for (Row row : resultSet) {
                String uid = row.getString("uid");
                session.execute("insert into uimg.uimge_statistic " +
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
                                "b1804, " +
                                "dt) " +
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
                                "?,?,?,?,?,?,?,?,?,?) ",
                        uid,
                        t1[0], t1[1], t1[2], t1[3], t1[4], t1[5],
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
                        t18[0], t18[1], t18[2], t18[3],
                        // 6 2 5 7 4 9 7 6 7 7 8 6 9 7 4 7 3 4
                        System.currentTimeMillis()
                );
                if (counter % 100000 == 0) {
                    System.out.println("uimge_statistic: " + counter);
                }
                counter++;
            }
            System.out.println("uimge_statistic: " + counter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        Uimge_Statistic client = new Uimge_Statistic();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);
        client.createSchema();
        String query2 = "select uid from uimg.uimge_manual_app2";

        client.loadData(query2);
        client.session.close();
        client.close();
    }
}
