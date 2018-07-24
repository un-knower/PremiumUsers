package com.ifeng.kafkauimg;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

@Slf4j
public final class UserProfileDumper implements Runnable, AutoCloseable {


    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String USER_PROFILE_TOPIC = "cimg";
    private static final int BATCH_SIZE = 100;

    private final ThreadLocal<List<UserProfile>> arrayListThreadLocal
            = ThreadLocal.withInitial(() -> new ArrayList<>(BATCH_SIZE + 1));
    private final KafkaStreams kafkaStreams;
//    private final JedisPool jedisPool;

    {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<byte[], byte[]> stream = builder.stream(USER_PROFILE_TOPIC);
        stream.foreach((k, v) -> {
            List<UserProfile> list = arrayListThreadLocal.get();
            if (list.size() >= BATCH_SIZE) {
                processData(list);
                list.clear();
            }
            try {
                UserProfile profile = objectMapper.readValue(v, UserProfile.class);
                if (profile != null)
                    list.add(profile);
            } catch (IOException e) {
                log.error(e.getMessage());
            }
        });

        this.kafkaStreams = new KafkaStreams(builder.build(), getStreamConfig());
//        JedisPoolConfig config = new JedisPoolConfig();
//        this.jedisPool = new JedisPool(config, "10.80.71.147",
//                6379, 2000, "dmd3#fjmh2sJ999gmnjka678i");
    }

    private static Properties getStreamConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaUtils.getIdsBrokerList());
        properties.put("group.id","test_uimg");
        properties.put("application.id", UserProfileDumper.class.getSimpleName());
        properties.put("client.id", "user_profile_dumper_client");
        properties.put("default.key.serde", Serdes.ByteArray().getClass().getName());
        properties.put("default.value.serde", Serdes.ByteArray().getClass().getName());
        properties.put("commit.interval.ms", 10 * 1000);
        properties.put("state.dir", "./kafka_state");
        // Read the topic from the very beginning if no previous consumer offsets are found for this app.
        // Resetting an app will set any existing consumer offsets to zero,
        // so setting this config combined with resetting will cause the application to re-process all the input data in the topic.
        properties.put("auto.offset.reset", "earliest");

        return properties;
    }

    public static void main(String[] args) {
        UserProfileDumper dumper = new UserProfileDumper();
        try {
            dumper.run();
        } finally {
            Runtime.getRuntime().addShutdownHook(new Thread(dumper::close));
        }
    }

    private void processData(List<UserProfile> dataList) {
        /*
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            for (UserProfile userProfile : dataList) {
                if (userProfile.uid == null)
                    continue;
                String key = "user:newsclient:".concat(userProfile.uid);
                Map<String, String> map = userProfile.toRedisMap();
                if (ThreadLocalRandom.current().nextInt(100) == 71)
                    log.info(map.toString());
                pipeline.hmset(key, map);
                pipeline.expire(key, 60 * 60 * 24 * 15);
            }
            pipeline.sync();
        }
        */

    }

    @Override
    public void run() {
        kafkaStreams.cleanUp();
        kafkaStreams.start();
    }

    @Override
    public void close() {
        if (kafkaStreams != null)
            kafkaStreams.close();
//        if (jedisPool != null)
//            jedisPool.close();
    }

//    @Test
//    public void test() throws Exception {
//        try (InputStream in = new FileInputStream("/Users/zhaoyuyu/Downloads/用户画像kafka同步消息示例.txt")) {
//            UserProfile profile = objectMapper.readValue(in, UserProfile.class);
//            System.out.println(profile.t1);
//            System.out.println(UserProfile.getMostSignificantTag(profile.t1));
//        }
//    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @ToString(doNotUseGetters = true)
    private static final class UserProfile implements Serializable {

        private static final long serialVersionUID = 2682590562854660371L;
        @JsonProperty("uid")
        private String uid;
        @JsonProperty("likevideo")
        private String likevideo;
        @JsonProperty("utime")
        private String utime;
        @JsonProperty("sw_open")
        private String swOpen;
        @JsonProperty("umt")
        private String umt;
        @JsonProperty("vid_t3")
        private String vidT3;
        @JsonProperty("general_locDetail")
        private String generalLocDetail;
        @JsonProperty("recent_t1")
        private String recentT1;
        @JsonProperty("vid_t2")
        private String vidT2;
        @JsonProperty("recent_t2")
        private String recentT2;
        @JsonProperty("recent_t3")
        private String recentT3;
        @JsonProperty("vid_t1")
        private String vidT1;
        @JsonProperty("last_t2_sourceSims")
        private String lastT2SourceSims;
        @JsonProperty("recent_ucombineTag")
        private String recentUcombineTag;
        @JsonProperty("last_t1")
        private String lastT1;
        @JsonProperty("search")
        private String search;
        @JsonProperty("last_t2")
        private String lastT2;
        @JsonProperty("last_t3")
        private String lastT3;
        @JsonProperty("general_ub")
        private String generalUb;
        @JsonProperty("ucombineTag")
        private String ucombineTag;
        @JsonProperty("ua_r")
        private String uaR;
        @JsonProperty("ub_sourceSims")
        private String ubSourceSims;
        @JsonProperty("loc")
        private String loc;
        @JsonProperty("upoi")
        private String upoi;
        @JsonProperty("last_dis_t1")
        private String lastDisT1;
        @JsonProperty("last_utime")
        private String lastUtime;
        @JsonProperty("user_schannel")
        private String userSchannel;
        @JsonProperty("last_dis_ucombineTag")
        private String lastDisUcombineTag;
        @JsonProperty("first_in")
        private String firstIn;
        @JsonProperty("slide_t2")
        private String slideT2;
        @JsonProperty("slide_t3")
        private String slideT3;
        @JsonProperty("last_in")
        private String lastIn;
        @JsonProperty("last_dis_t3")
        private String lastDisT3;
        @JsonProperty("slide_t1")
        private String slideT1;
        @JsonProperty("last_dis_t2")
        private String lastDisT2;
        @JsonProperty("last_uTopic")
        private String lastUTopic;
        @JsonProperty("t3")
        private String t3;
        @JsonProperty("t2")
        private String t2;
        @JsonProperty("t1")
        private String t1;
        @JsonProperty("group_ub")
        private String groupUb;
        @JsonProperty("uver")
        private String uver;
        @JsonProperty("last_cotagSims")
        private String lastCotagSims;
        @JsonProperty("net")
        private String net;
        @JsonProperty("cotagSims")
        private String cotagSims;
        @JsonProperty("long_uTopic")
        private String longUTopic;
        @JsonProperty("locDetail")
        private String locDetail;
        @JsonProperty("recent_cotagSims")
        private String recentCotagSims;
        @JsonProperty("uLevel")
        private String uLevel;
        @JsonProperty("ui")
        private String ui;
        @JsonProperty("uk")
        private String uk;
        @JsonProperty("e")
        private String e;
        @JsonProperty("general_ipList")
        private String generalIpList;
        @JsonProperty("umos")
        private String umos;
        @JsonProperty("topic1_explore")
        private String topic1Explore;
        @JsonProperty("iphone")
        private String iphone;
        @JsonProperty("ua_v")
        private String uaV;
        @JsonProperty("last_ucombineTag")
        private String lastUcombineTag;
        @JsonProperty("ip")
        private String ip;
        @JsonProperty("s")
        private String s;
        @JsonProperty("recent_uTopic")
        private String recentUTopic;
        @JsonProperty("ub")
        private String ub;
        @JsonProperty("ua")
        private String ua;
        @JsonProperty("sexpand")
        private String sexpand;
        @JsonProperty("t2_sourceSims")
        private String t2SourceSims;
        @JsonProperty("lastitems")
        private String lastitems;
        @JsonProperty("generalloc")
        private String generalloc;

        private static String getMostSignificantTag(String sList) {
            if (sList == null || sList.isEmpty())
                return null;
            String[] a = StringUtils.split(sList, '#');
            int maxLen = Math.min(5, a.length);
            PriorityQueue<Tag> queue = new PriorityQueue<>(maxLen);
            for (String s : a) {
                Tag tag = Tag.fromString(s);
                if (tag == null)
                    continue;
                if (queue.size() < maxLen)
                    queue.add(tag);
                else {
                    Tag top = queue.peek();
                    assert top != null;
                    if (top.compareTo(tag) < 0) {
                        queue.poll();
                        queue.add(tag);
                    }
                }
            }
            String[] temp = new String[maxLen];
            int len = queue.size();
            for (int i = 0; i < len; i++)
                temp[len - i - 1] = queue.poll().name;
            return StringUtils.join(temp, '|');
        }

        private Map<String, String> toRedisMap() {
            if (uid == null)
                return Collections.emptyMap();
            Map<String, String> map = new HashMap<>(7);
            map.put("uid", uid);
            if (StringUtils.isNotEmpty(userSchannel))
                map.put("user_schannel", userSchannel);
            if (StringUtils.isNotEmpty(uaR))
                map.put("ua_r", uaR);
            if (StringUtils.isNotEmpty(uaV))
                map.put("ua_v", uaV);
            if (StringUtils.isNotEmpty(ua))
                map.put("ua", ua);
            if (StringUtils.isNotEmpty(uver))
                map.put("uver", uver);
            String t1 = getMostSignificantTag(this.t1);
            if (t1 != null)
                map.put("t1", t1);
            return map;
        }
    }

    private static class Tag implements Serializable, Comparable<Tag> {
        private static final long serialVersionUID = -2684558425696912274L;
        private final String name;
        private final float score;

        Tag(String name, float score) {
            this.name = name;
            this.score = score;
        }

        static Tag fromString(String s) {
            String[] a = StringUtils.split(s, '_');
            if (a.length != 4)
                return null;
            String name = a[0];
            float score = Float.parseFloat(a[3]);
            return new Tag(name, score);
        }

        @Override
        public int compareTo(Tag o) {
            return Float.compare(score, o.score);
        }
    }


}
