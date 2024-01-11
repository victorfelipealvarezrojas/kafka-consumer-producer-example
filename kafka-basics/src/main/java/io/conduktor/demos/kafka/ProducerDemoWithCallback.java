package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Initial Process");
        Properties config = new Properties();
        config.setProperty("bootstrap.servers", "localhost:9092");
        config.setProperty("key.serializer", StringSerializer.class.getName());
        config.setProperty("value.serializer", StringSerializer.class.getName());
        config.setProperty("batch.size", "10000");
        // config.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).forEach(x -> {

            Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20).forEach(e -> {
                ProducerRecord<String, String> produceRecord = new ProducerRecord<>("demo_java", "hello word v3" + x  + e);
                producer.send(produceRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // execute every time a record successfully sent or an exception is thrown
                        boolean hasException = (exception != null);

                        if (!hasException)
                            log.info("Received new metadata \n" +
                                    "Topic    :" + metadata.topic() + "\n" +
                                    "Offset   :" + metadata.offset() + "\n" +
                                    "Partition:" + metadata.partition() + "\n" +
                                    "Timestamp:" + metadata.timestamp()


                            );

                        if (hasException) log.error("Error while producing", exception);
                    }
                });
            });

            try {
                Thread.sleep(1000);
            } catch (Exception err) {
                err.printStackTrace();
            }

            producer.flush();
            producer.close();

        });
    }

}
