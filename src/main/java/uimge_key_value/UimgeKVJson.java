package uimge_key_value;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.json.JSONObject;

public class UimgeKVJson {
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

    private void loadData(String query) {
        ResultSet resultSet = getSession().execute(query);
        int counter = 1;

        try {
            for (Row row : resultSet) {
                String key = row.getString("key");
                String value = row.getString("value");
                JSONObject json = new JSONObject(value);
//                String t1 = json.getJSONArray("t1");
//                String lsj = json.getString("lsj");
                String utime = json.getString("utime");
                System.out.println(key + " utime: " + utime+ " t1: " + json.getString("t1"));
            }
            System.out.println("vehicle_libsvm_t1t2: " + counter);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        UimgeKVJson client = new UimgeKVJson();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);

        String query1 = "select key,value from uimge_key_value limit 5";

        client.loadData(query1);
        client.session.close();
        client.close();
    }
}
