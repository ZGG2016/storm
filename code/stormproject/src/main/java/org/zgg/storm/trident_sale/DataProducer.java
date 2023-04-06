package org.zgg.storm.trident_sale;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

public class DataProducer {
    public static void main( String[] args ){
        Properties props = new Properties();
        props.put("bootstrap.servers", "datanode1:9092,datanode2:9092,datanode3:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Random random = new Random();
        String[] order_amt = {"10.10","20.10","50.2","60.0","80.1"};
        String[] province_id = {"1","2","3","4","5","6","7","8"};
        String[] user_id = {"1123","2132","3345","4456","567","678","789","856"};

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for(int i = 0; i < 150000; i++){

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String message = i+"\t"+order_amt[random.nextInt(5)]
                    +"\t"+province_id[random.nextInt(8)]
                    +"\t"+user_id[random.nextInt(8)];
            producer.send(new ProducerRecord<String, String>("protopic", message));

    }
        producer.close();
    }
}
