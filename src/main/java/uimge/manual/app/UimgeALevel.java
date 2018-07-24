package uimge.manual.app;

import com.datastax.driver.core.*;

import java.text.SimpleDateFormat;
import java.util.Date;

/*
 * LSJ 2018/05/24
 * uimge_alevel
 * 点击用户(groups.people_groups)导入成A级标签
 * */

public class UimgeALevel {

    private Cluster cluster;
    private Session session;
    private static final String INSERT_ALEVEL = "insert into groups.uimge_alevel" +
            "(uid,ua,a101,a102,a103,a104,a105,a106,a107," +
            "a201,a202,a203," +
            "a301,a302,a303,a304,a305,a306," +
            "a401,a402,a403,a404,a405,a406,a407," +
            "a501,a502,a503,a504,a505) " +
            "VALUES (" +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?," +
            "?,?,?,?,?,?,?,?,?,?);";
//    private static final String ALEVEL_C600 = "insert into groups.uimge_alevel" +
//            "(uid, a601,a602,a603,a604,a605,a606,a607,a608,a609,a610) " +
//            "VALUES (?,?,?,?,?,?,?,?,?,?,?);";

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
        PreparedStatement prepareStatement = session.prepare(INSERT_ALEVEL);

        int counter = 0;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        try {
            for (Row row : resultSet) {
                String uid = row.getString("uid");
                String ua = row.getString("ua");
                String cateid = row.getString("category_id");
                BoundStatement bindStatement = new BoundStatement(prepareStatement)
                        .bind(uid,ua,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1);
                session.execute(bindStatement);
//                session.execute("insert into groups.uimge_alevel" +
//                                "(uid,ua,a101,a102,a103,a104,a105,a106,a107," +
//                                "a201,a202,a203," +
//                                "a301,a302,a303,a304,a305,a306," +
//                                "a401,a402,a403,a404,a405,a406,a407," +
//                                "a501,a502,a503,a504,a505) " +
//                                "VALUES (" +
//                                "?,?,?,?,?,?,?,?,?,?," +
//                                "?,?,?,?,?,?,?,?,?,?," +
//                                "?,?,?,?,?,?,?,?,?,?) ",
//                        uid,ua,
//                        1,1,1,1,1,1,1,1,1,1,
//                        1,1,1,1,1,1,1,1,1,1,
//                        1,1,1,1,1,1,1,1
//                );

                switch(cateid) {
                    case "c600":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a601,a602,a603,a604,a605,a606,a607,a608,a609,a610) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1,1,1,1,1,1
                        );
                        break;
                    case "c700":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a701,a702,a703,a704,a705,a706,a707,a708) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1,1,1,1
                        );
                        break;
                    case "c800":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a801,a802,a803,a804,a805,a806,a807) " +
                                        "VALUES (?,?,?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1,1,1
                        );
                        break;
                    case "c900":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a901,a902,a903,a904,a905,a906,a907,a908) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1,1,1,1
                        );
                        break;
                    case "c1000":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a1001,a1002,a1003,a1004,a1005,a1006,a1007,a1008) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1,1,1,1
                        );
                        break;
                    case "c1100":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a1101,a1102,a1103,a1104,a1105,a1106,a1107,a1108,a1109) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1,1,1,1,1
                        );
                        break;
                    case "c1200":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a1201,a1202,a1203,a1204,a1205,a1206,a1207) " +
                                        "VALUES (?,?,?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1,1,1
                        );
                        break;
                    case "c1300":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a1301,a1302,a1303,a1304,a1305,a1306,a1307,a1308,a1309,a1310) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1,1,1,1,1,1
                        );
                        break;
                    case "c1400":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a1401,a1402,a1403,a1405,a1406,a1407,a1408,a1409) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1,1,1,1
                        );
                        break;
                    case "c1500":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a1501,a1502,a1503,a1504,a1505) " +
                                        "VALUES (?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1
                        );
                        break;
                    case "c1600":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a1601,a1602,a1603,a1604,a1605,a1606,a1607,a1608) " +
                                        "VALUES (?,?,?,?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1,1,1,1
                        );
                        break;
                    case "c1700":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a1701,a1702,a1703,a1704) " +
                                        "VALUES (?,?,?,?,?) ",
                                uid,
                                1,1,1,1
                        );
                        break;
                    case "c1800":
                        session.execute("insert into groups.uimge_alevel" +
                                        "(uid, a1801,a1802,a1803,a1804,a1805) " +
                                        "VALUES (?,?,?,?,?,?) ",
                                uid,
                                1,1,1,1,1
                        );
                        break;
                    default: break;
                }
                if (counter % 100000 == 0) {
                    System.out.println("uimge_alevel: " + counter +
                            " Time: " + dateFormat.format(new Date()));
                }
                counter++;
            }
            System.out.println("uimge_alevel: " + counter +
                    " Time: " + dateFormat.format(new Date()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        UimgeALevel client = new UimgeALevel();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.23.155", "10.80.24.155", "10.80.25.155"};
        int port = 9042;
        client.connect(contact_points, port);

        String query2 = "select * from groups.people_groups where label = '2' allow filtering";

        client.loadData(query2);

        client.session.close();
        client.close();
    }
}
