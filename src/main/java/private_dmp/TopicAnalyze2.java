package private_dmp;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.*;
import java.util.Map.Entry;

import static javax.swing.UIManager.getInt;

public class TopicAnalyze2 {

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

    public Map<String, Integer> sortMapByKey(Map<String, Integer> oriMap) {
        if (oriMap == null || oriMap.isEmpty()) {
            return null;
        }

        Map<String, Integer> sortedMap = new TreeMap<>(new Comparator<String>() {
            public int compare(String key1, String key2) {
                int intKey1 = 0, intKey2 = 0;
                try {
                    intKey1 = getInt(key1);
                    intKey2 = getInt(key2);
                } catch (Exception e) {
                    intKey1 = 0;
                    intKey2 = 0;
                }
                return intKey1 - intKey2;
            }});
        sortedMap.putAll(oriMap);
        return sortedMap;
    }

    private void loadData(String query) {

        ResultSet resultSet = getSession().execute(query);
        int counter = 0;
        int i = 30;
        Map<String, Integer> addMap = new HashMap<>();
        Map<Integer, Integer> mapKey = new HashMap<>();
        Map<Integer, String> mapValue = new HashMap<>();

        try {
            for (Row row : resultSet) {
                List<String> general_search = row.getList("general_search", String.class);
                Map<String, Integer> aMap = new HashMap<>();
                List<String> wordList = new ArrayList<>();
                for (String s : general_search) {
                    String str = s.split("\\|")[0];
                    if (!(str.equals("null")) && !(str.equals("$")) && !(str.equals("")) && !(str.equals(" "))) {
                        if (!wordList.contains(str)) {
                            wordList.add(str);
                        }
                    }
                }
                for(String s : wordList) {
                    if (addMap.containsKey(s)) {
                        addMap.put(s, (addMap.get(s) + 1));
                    } else {
                        addMap.put(s, 1);
                    }
                }

                if (counter % 10000 == 0) {
                    System.out.println("TopicAnalyze: " + counter);
                }
                counter++;
            }

            List<Entry<String, Integer>> list = new ArrayList<>(addMap.entrySet());

            Collections.sort(list,new Comparator<Map.Entry<String,Integer>>() {
                //倒序排序
                public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            if(list.size() > 30) {
                for (Entry<String, Integer> e : list.subList(1, 100)) {
                    System.out.println(e.getKey() + ":" + e.getValue());
                }
            }else {
                for (Entry<String, Integer> e : list.subList(1, list.size())) {
                    System.out.println(e.getKey() + ":" + e.getValue());
                }
            }

            System.out.println("TopicAnalyze: " + counter);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void close() {
        cluster.close();
    }

    public static void main(String[] args) {

        TopicAnalyze2 client = new TopicAnalyze2();
        String[] contact_points = {"10.80.17.155", "10.80.18.155", "10.80.19.155", "10.80.20.155",
                "10.80.21.155", "10.80.22.155", "10.80.23.155", "10.80.24.155"};
        int port = 9042;
        client.connect(contact_points, port);
        String query1 = "select * from uimg.private_shede_groups";

        client.loadData(query1);
        client.session.close();
        client.close();
    }
}





