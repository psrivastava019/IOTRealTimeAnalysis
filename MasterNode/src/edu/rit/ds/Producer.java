package edu.rit.ds;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class Producer {
	 public static void main(String[] args){
	        Properties properties = new Properties();
	        properties.put("bootstrap.servers", "localhost:9092");
	        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	        KafkaProducer kafkaProducer = new KafkaProducer(properties);
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
