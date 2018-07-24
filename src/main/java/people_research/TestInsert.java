package people_research;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.text.SimpleDateFormat;
import java.util.Date;

public class TestInsert {

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
//        int counter = 1;
        try {
            for (Row row : resultSet) {
                String uid = row.getString("id");
                int f100 = row.getInt("f100");
                System.out.println("id: " + uid + " ** f100: " + f100);
                session.execute("insert into uimg.test4" +
                                "(id,z) " +
                                "VALUES (" +
                                "?,?) ",
                        uid, f100
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {
        TestInsert client = new TestInsert();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.23.155", "10.80.24.155", "10.80.25.155"};
        int port = 9042;
        client.connect(contact_points, port);
        String query2 = "select * from uimg.test1";

        client.loadData(query2);

        client.session.close();
        client.close();
    }
}

