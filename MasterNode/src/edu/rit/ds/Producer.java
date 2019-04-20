package edu.rit.ds;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.ds2019.workLoadGenerator.KafkaJsonSerializer;


public class Producer {
	 public static void main(String[] args){
	        Properties properties = new Properties();
	        properties.put("bootstrap.servers", "localhost:9092");
	        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	        KafkaProducer kafkaProducer = new KafkaProducer(properties,new StringSerializer(), new KafkaJsonSerializer());
	        try{
	        	int i=0;
	            while(true){
	                System.out.println(i);
	                kafkaProducer.send(new ProducerRecord("test", Integer.toString(i), "test message - " + i++ ));
	            }
	        }catch (Exception e){
	            e.printStackTrace();
	        }finally {
	            kafkaProducer.close();
	        }
	    }
}
class KafkaJsonSerializer implements Serializer {


    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsBytes(o);
        } catch (Exception e) {
        }
        return retVal;
    }

    @Override
    public void close() {

    }

}
