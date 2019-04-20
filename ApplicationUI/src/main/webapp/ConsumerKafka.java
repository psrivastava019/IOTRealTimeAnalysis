package main.webapp;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ConsumerKafka {
	
	/*private Consumer<Integer, String> kafkaConsumer;
    private String topic;
    private String filePath;
    private BufferedWriter buffWriter;
    
    public void FileConsumer(String topic, String filePath) {

//    	super("FileConsumer", false);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "FileConsumer");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        kafkaConsumer = new KafkaConsumer<Integer, String>(properties);
        this.topic = topic;
        this.filePath = filePath;

        try {
            this.buffWriter = new BufferedWriter(new FileWriter(filePath));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void doWork() {
        // TODO Auto-generated method stub
        kafkaConsumer.subscribe(Collections.singletonList(this.topic));
        while(true) {
        	 ConsumerRecords<Integer, String> consumerRecords = kafkaConsumer.poll(1000);
             try {
                 for (ConsumerRecord<Integer, String> record : consumerRecords) 
                 {
                	 buffWriter.write(record.value() + System.lineSeparator());
                     System.out.println(record.value());
                 }
                     
                 buffWriter.flush();
             } catch (IOException e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
             }
             kafkaConsumer.commitAsync();
        }
       
    }

    public String name() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isInterruptible() {
        return false;
    }
	
	
    public static void main(String... args) throws Exception {
    	ConsumerKafka obj=new ConsumerKafka();
    	obj.FileConsumer("test1", "C:\\Users\\priya\\assignmentWorkspace\\ApllicationServer\\src\\edu\\rit\\ds\\sensorData.txt");
    	obj.doWork();
    }
	
	*/
	
	DataStream<String> finalStream;
	public void transform() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "group01");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer09<>("test2", new SimpleStringSchema(), properties));
		finalStream=messageStream.filter(x->x!=null);
//		finalStream.print();
		//		messageStream.filter(x->x!=null).
//		InputStream stream= (InputStream) messageStream;
//		Reader  inputStreamReader = new InputStreamReader(messageStream);
		env.execute();
	}
	
	public DataStream<String> callTransform(){
		 return finalStream;
	}
	
	public static void main(String args[]) throws Exception {
		ConsumerKafka consumer = new ConsumerKafka();
		consumer.transform();
	}	
	
	
	
	
	
/*	private final static String TOPIC = "test1";
    private final static String BOOTSTRAP_SERVERS ="localhost:9092";
    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                                    "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }
    
    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        final int giveUp = 100;   int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count()==0) {
            	System.out.println("Here");
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
//            	writeDataToFile(record.value());
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
    
    public static void writeDataToFile(String completMessage) {
    	File file=new File("C:\\Users\\priya\\assignmentWorkspace\\ApllicationServer\\src\\edu\\rit\\ds\\sensorData.txt");
    	try {
			FileWriter fw=new FileWriter(file,true);
			fw.write(completMessage);
			fw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    public static void main(String... args) throws Exception {
        runConsumer();
    }*/
}
