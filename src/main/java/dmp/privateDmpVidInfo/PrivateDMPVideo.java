package dmp.privateDmpVidInfo;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.json.JSONObject;

import java.math.BigInteger;
import java.util.Map;

public class PrivateDMPVideo {
    private Cluster cluster;

    private Session session;

    private Session getSession() {
        return session;
    }

    private void connect(String[] node, int port) {
        cluster = Cluster.builder().addContactPoints(node).withPort(port).build();
        cluster.getConfiguration().getQueryOptions().setFetchSize(50000);
        this.session = cluster.connect("dmp");
    }

    private void loadData(String query) {
        ResultSet resultSet = getSession().execute(query);
        int counter = 0;

        try {
            for (Row row : resultSet) {
                String md5 = row.getString("md5");
                String ts = row.getString("ts");
                long seqno = row.getLong("seqno");
                String action = row.getString("action");
                String mob = row.getString("mob");
                String network = row.getString("network");
                String os = row.getString("os");

                Map<String,String> others = row.getMap("others", String.class, String.class);
                Map<String,String> propsmap = row.getMap("propsmap", String.class, String.class);

                if(action.equals("v")) {
                    session.execute("insert into dmp.private_dmp_vid_info " +
                                    "(md5, ts,seqno,action,mob,network,os,others,propsmap) " +
                                    "VALUES (?,?,?,?,?,?,?,?,?) ",
                            md5, ts, seqno, action, mob, network, os,
                            others, propsmap
                    );
                }

            }
            if (counter % 100000 == 0) {
                System.out.println("PrivateDMPVideo: " + counter);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        PrivateDMPVideo client = new PrivateDMPVideo();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155", "10.80.25.155"};
        int port = 9042;
        client.connect(contact_points, port);
        String query1 = "select * from dmp.userlog6video";

        client.loadData(query1);
        client.session.close();
        client.close();
    }
}

