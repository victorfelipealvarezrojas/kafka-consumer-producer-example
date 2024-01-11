package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Initial Process");
        Properties config = new Properties();
        config.setProperty("bootstrap.servers", "localhost:9092");
        config.setProperty("key.serializer", StringSerializer.class.getName());
        config.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        Arrays.asList(1, 2).forEach(x -> {
            Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).forEach(e -> {
                String topic = "demo_java";
                String key = "is_" + e;
                String Value = "hello word v" + e;
                ProducerRecord<String, String> produceRecord = new ProducerRecord<>(topic, key, Value);
                producer.send(produceRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // execute every time a record successfully sent or an exception is thrown
                        boolean hasException = (exception != null);

                        if (!hasException)
                            log.info("Received new metadata \n" +
                                    "Topic    :" + metadata.topic() + "\n" +
                                    "Key      :" + key + "\n" +
                                    "Offset   :" + metadata.offset() + "\n" +
                                    "Partition:" + metadata.partition() + "\n" +
                                    "Timestamp:" + metadata.timestamp()
                            );

                        if (hasException) log.error("Error while producing", exception);
                    }
                });
            });
        });

        producer.flush();
        producer.close();
    }

}
