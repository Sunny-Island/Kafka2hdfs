package consumer;
import javassist.bytecode.ByteArray;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.util.Properties;

/**
 * Flink consumer topic data and store into hdfs.
 *
 * @author zhaojiabei.
 *
 *         Created by 11, 28, 2020
 */
public class kafka2hdfsBaseLine {

    private static Logger logger = LoggerFactory.getLogger(kafka2hdfsBaseLine.class);

    public static void main(String[] args) throws Exception {
        int batchSize = 256;
        int interval = 2000;
        if(args.length == 2){
            batchSize = Integer.parseInt(args[0]);
            interval = Integer.parseInt(args[1]);
        }
        String bootStrapServer = "172.21.0.12:9092,172.21.0.16:9092,172.21.0.6:9092";
        String hdfsPath = "/home/sankuai/out/writetohdfs";
        int parallelism = 10;

        logger.info("start executing");
        System.out.println("execute success");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setParallelism(parallelism);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>("test"
                , new SimpleStringSchema(), configByKafkaServer(bootStrapServer));
        consumer.setStartFromLatest();
        DataStream<String> source = env.addSource(consumer);
        //DataStream<String> count = source.map(new CounterMapper());
        BucketingSink<String> sink = new BucketingSink<>(hdfsPath);
        sink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd"));
        sink.setBatchSize(1024 * 1024 * batchSize); // this is 128M
        sink.setBatchRolloverInterval(interval); // one second producer a file into hdfs
        source.addSink(sink).disableChaining();
        env.execute("Kafka2Hdfs");
        logger.info("execute successfully");
        System.out.println("execute success");
    }

    private static Properties configByKafkaServer(String bootStrapServer) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootStrapServer);
        props.setProperty("zookeeper.connect","172.21.0.12:2181,172.21.0.16:2181,172.21.0.6:2181");
        props.setProperty("group.id", "test_bll_group");
        props.put("key.deserializer", IntegerDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        return props;
    }

}
