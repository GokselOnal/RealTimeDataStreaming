package Kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.mongodb.BasicDBObject;
import java.util.Properties;
import java.util.Arrays;
import Mongo.MongoDB;



public class KafkaConsumer {
    public static void main(String[] args) {
        String topicName="streaming";
        Properties configPro = new Properties();
        configPro.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configPro.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configPro.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configPro.put(ConsumerConfig.GROUP_ID_CONFIG,"streaming_group");
        configPro.put(ConsumerConfig.CLIENT_ID_CONFIG,"streaming_client");

        MongoDB mongo_db = new MongoDB("db_stream", "users");

        org.apache.kafka.clients.consumer.KafkaConsumer<String,String> kafkaConsumer;
        kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(configPro);

        kafkaConsumer.subscribe(Arrays.asList(topicName));
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String[] data = record.value().split(",");
                    BasicDBObject obj=new BasicDBObject()
                            .append("first_name",data[0])
                            .append("last_name",data[1])
                            .append("age",data[2])
                            .append("job",data[3]);
                    mongo_db.add_record(obj);
                }
            }
        }
        catch (Exception e) {
            System.out.println(e);
        }
        kafkaConsumer.close();
    }
}
