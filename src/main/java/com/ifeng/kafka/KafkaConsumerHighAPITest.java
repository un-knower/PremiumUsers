//package com.ifeng.kafka;
//
//public class KafkaConsumerHighAPITest {
//    public static void main(String[] args) {
//        String[] zookeeper = {"10.80.2.161:9092",
//                "10.80.5.161:9092",
//                "10.80.8.161:9092",
//                "10.80.11.161:9092",
//                "10.80.14.161:9092",
//                "10.80.17.161:9092",
//                "10.80.20.161:9092",
//                "10.80.23.161:9092",
//                "10.80.26.161:9092",
//                "10.80.29.161:9092",
//                "10.80.32.161:9092",};
//        String groupId = "group1";
//        String topic = "uimg_test";
//        int threads = 1;
//
//        KafkaConsumerHighAPI example = new KafkaConsumerHighAPI(topic, threads, zookeeper, groupId);
//        new Thread(example).start();
//
//        // 执行10秒后结束
//        int sleepMillis = 600000;
//        try {
//            Thread.sleep(sleepMillis);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        // 关闭
//        example.shutdown();
//    }
//}
