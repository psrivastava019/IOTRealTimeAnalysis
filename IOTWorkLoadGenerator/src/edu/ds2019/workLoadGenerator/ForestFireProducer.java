package edu.ds2019.workLoadGenerator;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ForestFireProducer implements Runnable {
	
	private ArrayList<Integer> coordinates;
	private KafkaProducer producer=null;
	public ForestFireProducer(ArrayList<Integer> coordinates) {
		Properties properties = new Properties();
	    properties.put("bootstrap.servers", "localhost:9092");
	    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		this.coordinates = coordinates;
		producer=new KafkaProducer(properties);
	}
	
	@Override
	public void run() {
		while(true) {
			this.producer.send(new ProducerRecord("test", this.coordinates.get(0)+""+this.coordinates.get(1)+""));
			System.out.println("Alert Fire at: "+ this.coordinates);
			try {
				Thread.currentThread().sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}