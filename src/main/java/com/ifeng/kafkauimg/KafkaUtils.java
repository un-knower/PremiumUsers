package com.ifeng.kafkauimg;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

final class KafkaUtils {


    private KafkaUtils() {

    }

    static String getIdsBrokerList() {
        return StringUtils.join(getIdsKafkaHosts(), ',');
    }

    static String getArticleBrokerList() {
        return StringUtils.join(getArticleKafkaHosts(), ',');
    }

    private static List<String> getIdsKafkaHosts() {
        try (InputStream in = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("kafka_cluster.txt")) {
            List<String> hosts = new ArrayList<>();
            Scanner scanner = new Scanner(in, "UTF-8");
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                int index = line.indexOf('\t');
                String ip = line.substring(0, index);
                hosts.add(ip + ":9092");
            }
            scanner.close();
            return hosts;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<String> getArticleKafkaHosts() {
        return Arrays.asList(
                "10.90.11.19:19092",
                "10.90.11.32:19092",
                "10.90.11.45:19092",
                "10.90.11.47:19092",
                "10.90.11.48:19092"
        );
    }

    public static void main(String[] args) {
        System.out.println(getIdsBrokerList());
    }

}
