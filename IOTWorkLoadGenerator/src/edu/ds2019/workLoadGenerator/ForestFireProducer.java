package edu.ds2019.workLoadGenerator;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ForestFireProducer implements Runnable {
	
	
	static  KafkaProducer kafkaProducer=null;
	private IOTDevice device;
	public ForestFireProducer(IOTDevice device) {
		this.device = device;
	}
	
	@Override
	public void run() {
		while(true) {
			ObjectMapper mapper=new ObjectMapper();
			try {
				String json = mapper.writeValueAsString(this.device);
				kafkaProducer(json);
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(50000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	
	public static void kafkaProducer(String jsonString) {
		if(kafkaProducer==null) {
			Properties properties = new Properties();
			properties.put("bootstrap.servers", "localhost:9092");
			properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			kafkaProducer = new KafkaProducer(properties);
		}
		
		try {

			kafkaProducer.send(new ProducerRecord("test", jsonString));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
//			kafkaProducer.close();
		}
	}
}
