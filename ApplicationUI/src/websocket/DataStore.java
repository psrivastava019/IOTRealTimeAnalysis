package websocket;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import org.apache.flink.util.IterableIterator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import websocket.ConsumerEndPoint;

public class DataStore {

	private final static String TOPIC = "relayDCToAppServer";
    private final static String BOOTSTRAP_SERVERS ="localhost:9092";
    private static Consumer<Long, String> consumer = null;
    public static String jsonStringData = "";
    private static void setConsumerObject(){
    	final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                                    "group01");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put("auto.commit.interval.ms",1000);
        props.put("session.timeout.ms",30000);
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
    }
    
	
	  private static void setConsumerData() { 
		  ConsumerRecords<Long, String> consumerRecords=null; 
		  while(true) { 
			  consumerRecords = consumer.poll(100);
			  System.out.println("count:"+consumerRecords.count());
			  consumerRecords.forEach(record -> {
				  System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", 
						  record.key(),
						  record.value(), record.partition(), record.offset()); 
				  jsonStringData = record.value().toString(); 
				  System.out.println(jsonStringData); 
				  try {
					  Thread.sleep(2000); 
				  } catch (InterruptedException e) {
					  // TODO Auto-generated
					  e.printStackTrace(); 
				} 
			  }); 
		} 
	}
	 
    
   /* private String setConsumerData() {
    	setConsumerObject();
    	ConsumerRecords<Long, String> consumerRecords= consumer.poll(100);
    	Date recentDate = null;
    	String recentData = "";
    	Iterator it = consumerRecords.iterator();
    	ObjectMapper map = new ObjectMapper();
    	while(it.hasNext()) {
    		ConsumerRecord<Long, String> record = (ConsumerRecord<Long, String>) it.next();
    		IOTDeviceMaster master;
			try {
				master = map.readValue(record.value(), IOTDeviceMaster.class);
				if(recentDate == null) {
	    			recentDate = master.getDate();
	    			recentData = record.value();
	    		} else {
	    			if(recentDate.compareTo(master.getDate()) < 0) {
	    				recentDate = master.getDate();
	    				recentData = record.value();
	    			}
	    		}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		
    	}
    	System.out.println("recentData:"+recentData);
    	return recentData;
    }
    */
    public static void main(String[] args) {
		// TODO Auto-generated method stub
		setConsumerObject();
		setConsumerData();
	}

}
