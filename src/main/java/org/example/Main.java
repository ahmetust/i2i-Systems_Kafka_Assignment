
package org.example;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Main {
    public Main() {
    }

    public static void main(String[] args) {
        String topicName = "subscriber";
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "subscriber-group");
        props.put("key.deserializer", IntegerDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        try {
            Producer<Integer, String> producer = new KafkaProducer(props);
            try {
                Random random = new Random();
                int sayac = 0;
                do {
                    int subscId = random.nextInt(1000);
                    String subscName = "Subscriber " + subscId;
                    String subscSurname = "Surname " + subscId;
                    String msisdn = "MSISDN " + subscId;
                    String message = String.format("%d,%s,%s,%s", subscId, subscName, subscSurname, msisdn);
                    ProducerRecord<Integer, String> record = new ProducerRecord(topicName, subscId, message);
                    producer.send(record);
                    System.out.println("Message sent: " + message);
                    Thread.sleep(1000L);
                    ++sayac;
                } while(sayac != 10);
            } catch (Throwable var1) {
                throw var1;
            }
            producer.close();
        } catch (InterruptedException var2) {
            var2.printStackTrace();
        }
        Consumer<Integer, String> consumer = new KafkaConsumer(props);

            consumer.subscribe(Collections.singletonList(topicName));

            while(true) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100L));
                Iterator var3 = records.iterator();

                while(var3.hasNext()) {
                    ConsumerRecord<Integer, String> record = (ConsumerRecord)var3.next();
                    System.out.println("Received record: " + (String)record.value());
                }
            }
    }
}