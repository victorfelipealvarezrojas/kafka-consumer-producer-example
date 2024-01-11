package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Initial Process Consumer");
        Properties config = new Properties();
        config.setProperty("bootstrap.servers", "localhost:9092");
        config.setProperty("key.deserializer",    StringDeserializer.class.getName());
        config.setProperty("value.deserializer",  StringDeserializer.class.getName());
        config.setProperty("group.id",  "my-java-application");
        config.setProperty("auto.offset.reset",  "earliest");

        KafkaConsumer<String, String> _consumer = new KafkaConsumer<>(config);
        _consumer.subscribe(List.of("demo_java"));

        while (true) {
            log.info("polling");
            ConsumerRecords<String, String> records = _consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record: records){
                log.info("key:" + record.key() + ", Value: " + record.value());
                log.info("Partition:" + record.partition() + ", Offset: " + record.offset());
            }
        }


    }

}
