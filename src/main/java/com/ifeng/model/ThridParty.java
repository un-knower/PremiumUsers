package com.ifeng.model;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import sun.misc.BASE64Encoder;

import java.security.MessageDigest;

public class ThridParty {

    private Cluster cluster;

    private Session session;

    private Session getSession() {
        return session;
    }

    public static String MD5encode(String key) {
        char hexDigits[] = {
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
        };
        try {
            byte[] btInput = key.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            return null;
        }
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
//            MessageDigest md5= MessageDigest.getInstance("MD5");
//            BASE64Encoder base64en = new BASE64Encoder();
            for (Row row : resultSet) {
                String uid = row.getString("uid");
                String md = MD5encode(uid);
//                String md=base64en.encode(md5.digest(uid.getBytes("utf-8")));
                session.execute("insert into uimg.uimge_md5_uid " +
                                "(uid,md5) " +
                                "VALUES (?,?) ",
                        uid, md
                );
                if (counter % 100000 == 0) {
                    System.out.println("md5_uimge: " + counter);
                }
                counter++;
            }
            System.out.println("md5_uimge: " + counter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        ThridParty client = new ThridParty();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);

        String query2 = "select uid from uimg.uimge_allvalues";

        client.loadData(query2);

        client.session.close();
        client.close();
    }
}
