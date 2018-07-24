package people_research;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

/*
 * LSJ 2018/05/04
 * people_research
 * */

public class PeopleTags {

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
                String category_id = row.getString("category_id");
                String type;
                switch(category_id) {
                    case "c600": type = "教育";break;
                    case "c700": type = "旅游";break;
                    case "c800": type = "金融";break;
                    case "c900": type = "汽车";break;
                    case "c1000": type = "房产";break;
                    case "c1100": type = "游戏";break;
                    case "c1200": type = "体育";break;
                    case "c1300": type = "时尚网购";break;
                    case "c1400": type = "快消";break;
                    case "c1500": type = "美容时尚";break;
                    case "c1600": type = "科技数码";break;
                    case "c1700": type = "母婴";break;
                    case "c1800": type = "健康美食";break;
                    case "z10001": type = "高通";break;
                    case "z10002": type = "收藏";break;
                    case "z10003": type = "广丰双致";break;
                    case "z10004": type = "广本雅阁";break;
                    default: type = "其他";break;
                }

                int c6 = row.getInt("c601") + row.getInt("c602") + row.getInt("c603") + row.getInt("c604")
                        + row.getInt("c605") + row.getInt("c606") + row.getInt("c607") + row.getInt("c608") + row.getInt("c609");
                int c7 = row.getInt("c701") + row.getInt("c702") + row.getInt("c703") + row.getInt("c704")
                        + row.getInt("c705") + row.getInt("c706") + row.getInt("c707");
                int c8 = row.getInt("c801") + row.getInt("c802") + row.getInt("c803") + row.getInt("c804")
                        + row.getInt("c805") + row.getInt("c806");
                int c9 = row.getInt("c901") + row.getInt("c902") + row.getInt("c903") + row.getInt("c904")
                        + row.getInt("c905") + row.getInt("c906") + row.getInt("c907");
                int c10 = row.getInt("c1001") + row.getInt("c1002") + row.getInt("c1003") + row.getInt("c1004")
                        + row.getInt("c1005") + row.getInt("c1006") + row.getInt("c1007");
                int c11 = row.getInt("c1101") + row.getInt("c1102") + row.getInt("c1103") + row.getInt("c1104")
                        + row.getInt("c1105") + row.getInt("c1106") + row.getInt("c1107") + row.getInt("c1108");
                int c12 = row.getInt("c1201") + row.getInt("c1202") + row.getInt("c1203") + row.getInt("c1204")
                        + row.getInt("c1205") + row.getInt("c1206");
                int c13 = row.getInt("c1301") + row.getInt("c1302") + row.getInt("c1303") + row.getInt("c1304")
                        + row.getInt("c1305") + row.getInt("c1306") + row.getInt("c1307") + row.getInt("c1308") + row.getInt("c1309");
                int c14 = row.getInt("c1401") + row.getInt("c1402") + row.getInt("c1403")
                        + row.getInt("c1405") + row.getInt("c1406") + row.getInt("c1407") + row.getInt("c1408");
                int c15 = row.getInt("c1501") + row.getInt("c1502") + row.getInt("c1503") + row.getInt("c1504");
                int c16 = row.getInt("c1601") + row.getInt("c1602") + row.getInt("c1603") + row.getInt("c1604")
                        + row.getInt("c1605") + row.getInt("c1606") + row.getInt("c1607");
                int c17 = row.getInt("c1701") + row.getInt("c1702") + row.getInt("c1703");
                int c18 = row.getInt("c1801") + row.getInt("c1802") + row.getInt("c1803") + row.getInt("c1804");

//                String s6 = (c6>0?"教育_","" + (float) c6 / 9;
//                String s7 = "旅游_" + (float) c7 / 7;
//                String s8 = "金融_" + (float) c8 / 6;
//                String s9 = "汽车_" + (float) c9 / 7;
//                String s10 = "房产_" + (float) c10 / 7;
//                String s11 = "游戏_" + (float) c11 / 8;
//                String s12 = "体育_" + (float) c12 / 6;
//                String s13 = "时尚网购_" + (float) c13 / 9;
//                String s14 = "快消_" + (float) c14 / 7;
//                String s15 = "美容时尚_" + (float) c15 / 4;
//                String s16 = "科技数码_" + (float) c16 / 7;
//                String s17 = "母婴_" + (float) c17 / 3;
//                String s18 = "健康美食_" + (float) c18 / 4;

                String s6 = (c6>0?"教育,":"");
                String s7 = (c7>0?"旅游,":"");
                String s8 = (c8>0?"金融,":"");
                String s9 = (c9>0?"汽车,":"");
                String s10 = (c10>0?"房产,":"");
                String s11 = (c11>0?"游戏,":"");
                String s12 = (c12>0?"体育,":"");
                String s13 = (c13>0?"时尚网购,":"");
                String s14 = (c14>0?"快消,":"");
                String s15 = (c15>0?"美容时尚,":"");
                String s16 = (c16>0?"科技数码,":"");
                String s17 = (c17>0?"母婴,":"");
                String s18 = (c18>0?"健康美食,":"");

                String tags = " " + s6 + s7 + s8 + s9 + s10 + s11 + s12 + s13 + s14 + s15 + s16 + s17 + s18;
                if(tags != " ") tags = tags.substring(0,tags.length()-1);

                session.execute("insert into groups.people_taglist " +
                                "(uid, " +
                                "category, " +
                                "taglist) " +
                                "VALUES (?,?,?) ",
                        uid,
                        type,
                        tags
                );

                if (counter % 100000 == 0) {
                    System.out.println("people_taglist: " + counter);
                }
                counter++;
            }
            System.out.println("people_taglist: " + counter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        PeopleTags client = new PeopleTags();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);

        String query2 = "select * from groups.people_research";

        client.loadData(query2);

        client.session.close();
        client.close();
    }
}
