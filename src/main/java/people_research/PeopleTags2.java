package people_research;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/*
 * LSJ 2018/05/04
 * people_research
 * */

public class PeopleTags2 {

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
        int counter = 0;

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
                StringBuilder stringBuilder = new StringBuilder();

                stringBuilder.append(row.getInt("c601") == 1?"学前教育,":"");
                stringBuilder.append(row.getInt("c602") == 1?"中小学教育,":"");
                stringBuilder.append(row.getInt("c603") == 1?"大学考试,":"");
                stringBuilder.append(row.getInt("c604") == 1?"职业教育,":"");
                stringBuilder.append(row.getInt("c605") == 1?"学历教育,":"");
                stringBuilder.append(row.getInt("c606") == 1?"语言培训,":"");
                stringBuilder.append(row.getInt("c607") == 1?"IT培训,":"");
                stringBuilder.append(row.getInt("c608") == 1?"公务员考试,":"");
                stringBuilder.append(row.getInt("c609") == 1?"出国留学,":"");
                stringBuilder.append(row.getInt("c701") == 1?"港澳台旅游,":"");
                stringBuilder.append(row.getInt("c702") == 1?"国内游,":"");
                stringBuilder.append(row.getInt("c703") == 1?"境外游,":"");
                stringBuilder.append(row.getInt("c704") == 1?"户外探险,":"");
                stringBuilder.append(row.getInt("c705") == 1?"酒店住宿,":"");
                stringBuilder.append(row.getInt("c706") == 1?"机票,":"");
                stringBuilder.append(row.getInt("c707") == 1?"自驾游,":"");
                stringBuilder.append(row.getInt("c801") == 1?"股票基金,":"");
                stringBuilder.append(row.getInt("c802") == 1?"保险,":"");
                stringBuilder.append(row.getInt("c803") == 1?"彩票,":"");
                stringBuilder.append(row.getInt("c804") == 1?"期货外汇,":"");
                stringBuilder.append(row.getInt("c805") == 1?"银行理财,":"");
                stringBuilder.append(row.getInt("c806") == 1?"互联网金融,":"");
                stringBuilder.append(row.getInt("c901") == 1?"低档车,":"");
                stringBuilder.append(row.getInt("c902") == 1?"中档车,":"");
                stringBuilder.append(row.getInt("c903") == 1?"高档车,":"");
                stringBuilder.append(row.getInt("c904") == 1?"豪华车,":"");
                stringBuilder.append(row.getInt("c905") == 1?"二手车,":"");
                stringBuilder.append(row.getInt("c906") == 1?"租车,":"");
                stringBuilder.append(row.getInt("c907") == 1?"改装与保养,":"");
                stringBuilder.append(row.getInt("c1001") == 1?"普通房产,":"");
                stringBuilder.append(row.getInt("c1002") == 1?"别墅房产,":"");
                stringBuilder.append(row.getInt("c1003") == 1?"商用房,":"");
                stringBuilder.append(row.getInt("c1004") == 1?"二手房,":"");
                stringBuilder.append(row.getInt("c1005") == 1?"装修装饰,":"");
                stringBuilder.append(row.getInt("c1006") == 1?"家居建材,":"");
                stringBuilder.append(row.getInt("c1007") == 1?"个人租房,":"");
                stringBuilder.append(row.getInt("c1101") == 1?"客户端网游,":"");
                stringBuilder.append(row.getInt("c1102") == 1?"网页游戏,":"");
                stringBuilder.append(row.getInt("c1103") == 1?"手机游戏,":"");
                stringBuilder.append(row.getInt("c1104") == 1?"掌机游戏,":"");
                stringBuilder.append(row.getInt("c1105") == 1?"小游戏,":"");
                stringBuilder.append(row.getInt("c1106") == 1?"单机游戏,":"");
                stringBuilder.append(row.getInt("c1107") == 1?"电视游戏,":"");
                stringBuilder.append(row.getInt("c1108") == 1?"桌游,":"");
                stringBuilder.append(row.getInt("c1201") == 1?"球类运动,":"");
                stringBuilder.append(row.getInt("c1202") == 1?"健身,":"");
                stringBuilder.append(row.getInt("c1203") == 1?"水上运动,":"");
                stringBuilder.append(row.getInt("c1204") == 1?"跑步骑行,":"");
                stringBuilder.append(row.getInt("c1205") == 1?"极限运动,":"");
                stringBuilder.append(row.getInt("c1206") == 1?"体育用品,":"");
                stringBuilder.append(row.getInt("c1301") == 1?"女装,":"");
                stringBuilder.append(row.getInt("c1302") == 1?"男装,":"");
                stringBuilder.append(row.getInt("c1303") == 1?"运动服饰,":"");
                stringBuilder.append(row.getInt("c1304") == 1?"内衣,":"");
                stringBuilder.append(row.getInt("c1305") == 1?"女鞋,":"");
                stringBuilder.append(row.getInt("c1306") == 1?"男鞋,":"");
                stringBuilder.append(row.getInt("c1307") == 1?"童装/童鞋,":"");
                stringBuilder.append(row.getInt("c1308") == 1?"运动鞋,":"");
                stringBuilder.append(row.getInt("c1309") == 1?"珠宝首饰,":"");
                stringBuilder.append(row.getInt("c1401") == 1?"母婴用品,":"");
                stringBuilder.append(row.getInt("c1402") == 1?"药品,":"");
                stringBuilder.append(row.getInt("c1403") == 1?"个人洗护/卫生用品,":"");
                stringBuilder.append(row.getInt("c1405") == 1?"食品类,":"");
                stringBuilder.append(row.getInt("c1406") == 1?"饮品类,":"");
                stringBuilder.append(row.getInt("c1407") == 1?"烟草,":"");
                stringBuilder.append(row.getInt("c1408") == 1?"酒类,":"");
                stringBuilder.append(row.getInt("c1501") == 1?"减肥瘦身,":"");
                stringBuilder.append(row.getInt("c1502") == 1?"美容整形,":"");
                stringBuilder.append(row.getInt("c1503") == 1?"美发护肤,":"");
                stringBuilder.append(row.getInt("c1504") == 1?"化妆品,":"");
                stringBuilder.append(row.getInt("c1601") == 1?"电脑硬件,":"");
                stringBuilder.append(row.getInt("c1602") == 1?"数码摄像,":"");
                stringBuilder.append(row.getInt("c1603") == 1?"手机及配件,":"");
                stringBuilder.append(row.getInt("c1604") == 1?"网络设备与服务,":"");
                stringBuilder.append(row.getInt("c1605") == 1?"电玩/游戏机,":"");
                stringBuilder.append(row.getInt("c1606") == 1?"软件,":"");
                stringBuilder.append(row.getInt("c1607") == 1?"办公设备,":"");
                stringBuilder.append(row.getInt("c1701") == 1?"准妈妈,":"");
                stringBuilder.append(row.getInt("c1702") == 1?"婴幼儿妈妈,":"");
                stringBuilder.append(row.getInt("c1703") == 1?"儿童妈妈,":"");
                stringBuilder.append(row.getInt("c1801") == 1?"美食达人,":"");
                stringBuilder.append(row.getInt("c1802") == 1?"健康关注者,":"");
                stringBuilder.append(row.getInt("c1803") == 1?"美食特产,":"");
                stringBuilder.append(row.getInt("c1804") == 1?"保健食品,":"");


                String tags = stringBuilder.toString();
//                if(!tags.equals(" ") || tags != null || tags != "") tags = tags.substring(0,tags.length()-1);
                if(tags.length() > 0) tags = tags.substring(0,tags.length()-1);

                session.execute("insert into groups.people_taglist3 " +
                                "(uid, " +
                                "category, " +
                                "taglist) " +
                                "VALUES (?,?,?) ",
                        uid,
                        type,
                        tags
                );

                if (counter % 100000 == 0) {
                    System.out.println("people_taglist3: " + counter);
                }
                counter++;
            }
            System.out.println("people_taglist3: " + counter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        PeopleTags2 client = new PeopleTags2();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);

        String query2 = "select * from groups.people_research2";

        client.loadData(query2);

        client.session.close();
        client.close();
    }
}
