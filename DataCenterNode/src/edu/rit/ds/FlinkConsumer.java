package edu.rit.ds;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.JSONDeserializationSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.sling.commons.json.io.JSONWriter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import scala.util.control.Exception.Catch;

public class FlinkConsumer implements Serializable {
	public String arrafy(Map<String,ObjectNode> map) {
		JSONObject json= new JSONObject();
		List<ObjectNode> newList=new ArrayList<>();
		for(Map.Entry<String, ObjectNode> entry: map.entrySet()) {
			newList.add(entry.getValue());
		}
		json.put("iotData", newList);
		return json.toJSONString();
	}
	public void transform() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "group01");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer09<>("test1",  new SimpleStringSchema(), properties));
		
		FlinkKafkaProducer09<String> flinkKafkaProducer = createStringProducer("test1", "localhost:9092");
		
		
//		fw=new FileWriter(file);
		
		
		DataStream<Tuple2<String,String>> newMessageStream=messageStream.flatMap(new FlatMapFunction<String,Tuple2<String,String>>() {
//			@Override  
		
			//@Override
			/*@Override
			
			public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
				ObjectMapper mapper = new ObjectMapper();
				try {
					JsonNode rootNode = mapper.readValue(value, JsonNode.class);
					if (rootNode.isObject()) {
						ObjectNode obj = mapper.convertValue(rootNode, ObjectNode.class);
						if (obj.has("temp")) {
		                    System.out.println("a=" + obj.get("temp").asInt()+"fd"+obj.toString());
		                    if(obj.get("temp").asInt()>70) {
		                    	out.collect(Tuple2.of("test2", value.toString()));
		                    }
		                    else {
		                    	out.collect(Tuple2.of("test3", value.toString()));
		                    }
					}
				}
			}
				catch (java.io.IOException ex){
		            ex.printStackTrace();
		            
		        }
				
			}

		});    
		
		newMessageStream.addSink(new FlinkKafkaProducer09<>(
		        "test1",
		        new KeyedSerializationSchema<Tuple2<String, String>>() {

					@Override
					public byte[] serializeKey(Tuple2<String, String> element) {
						// TODO Auto-generated method stub
						byte[] b=element.f0.getBytes();
						return b;
					}

					@Override
					public byte[] serializeValue(Tuple2<String, String> element) {
						byte[] b=element.f1.getBytes();
						return b;
					}

					@Override
					public String getTargetTopic(Tuple2<String, String> element) {
						// TODO Auto-generated method stub
						return element.f0;
					}
		            
		        },
		        properties)
		);*/
			
		
			
		Map<String,ObjectNode> map=new HashMap<>();
		
		@Override
		
		public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
			ObjectMapper mapper = new ObjectMapper();
			try {
				JsonNode rootNode = mapper.readValue(value, JsonNode.class);
				if (rootNode.isObject()) {
					ObjectNode obj = mapper.convertValue(rootNode, ObjectNode.class);
					if (obj.has("temp")) {
	                    System.out.println("a=" + obj.get("temp").asInt()+"fd"+obj.toString());
	                    if(obj.get("temp").asInt()>70) {
	                    	map.put(obj.get("id").toString(), obj);
	                    	
	                    	out.collect(Tuple2.of("test2", arrafy(map)));
	                    }
	                    else {
	                    	map.put(obj.get("id").toString(), obj);
	                    	out.collect(Tuple2.of("test3", arrafy(map)));
	                    }
				}
			}
		}
			catch (java.io.IOException ex){
	            ex.printStackTrace();
	            
	        }
			
		}

	});    
	
	newMessageStream.addSink(new FlinkKafkaProducer09<>(
	        "test1",
	        new KeyedSerializationSchema<Tuple2<String, String>>() {

				@Override
				public byte[] serializeKey(Tuple2<String, String> element) {
					// TODO Auto-generated method stub
					byte[] b=element.f0.getBytes();
					return b;
				}

				@Override
				public byte[] serializeValue(Tuple2<String, String> element) {
					byte[] b=element.f1.getBytes();
					return b;
				}

				@Override
				public String getTargetTopic(Tuple2<String, String> element) {
					// TODO Auto-generated method stub
					return element.f0;
				}
	            
	        },
	        properties)
	);
		  /*  public String map(String value) throws Exception {
		    	
		        ObjectMapper mapper = new ObjectMapper();
		        try {
		        	JsonNode rootNode = mapper.readValue(value, JsonNode.class);
		            if (rootNode.isObject()) {
//		                ObjectNode obj = (ObjectNode) rootNode;
		            	ObjectNode obj = mapper.convertValue(rootNode, ObjectNode.class);
		                if (obj.has("temp")) {
		                    System.out.println("a=" + obj.get("temp").asInt()+"fd"+obj.toString());
		                    if(obj.get("temp").asInt()>72) {
		                    	return obj.toString();
		                    }
		                   else {
		                	   System.out.println("a=" + obj.get("temp").asInt());
//		                	   writeToFile(obj.toString());
		                	   if(file!=null ) {
		                		   fw=new FileWriter(file);
		                		   fw.write(obj.toString());
		                	   }
		                	   else {
		                		   file = new File("C:\\Users\\priya\\assignmentWorkspace\\DataCenterNode\\src\\edu\\rit\\ds\\sensorData.txt");
		                	   }
		                	    fw.write(obj.toString());
		                		 
		                    }
		                }
		            }
		            return null;
		        }catch (java.io.IOException ex){
		            ex.printStackTrace();
		            return null;
		        }
		    }*/

			
//		}).filter(x->x!=null ).addSink(flinkKafkaProducer);
		
		
		/*messageStream.map(new MapFunction<String, String>() {
		   

			@Override
		    
		    public String map(String value) throws Exception {
		    	
		        ObjectMapper mapper = new ObjectMapper();
		        try {
		        	JsonNode rootNode = mapper.readValue(value, JsonNode.class);
		            if (rootNode.isObject()) {
//		                ObjectNode obj = (ObjectNode) rootNode;
		            	ObjectNode obj = mapper.convertValue(rootNode, ObjectNode.class);
		                if (obj.has("temp")) {
		                    System.out.println("a=" + obj.get("temp").asInt()+"fd"+obj.toString());
		                    if(obj.get("temp").asInt()>72) {
		                    	return obj.toString();
		                    }
		                   else {
		                	   System.out.println("a=" + obj.get("temp").asInt());
//		                	   writeToFile(obj.toString());
		                	   if(file!=null ) {
		                		   fw=new FileWriter(file);
		                		   fw.write(obj.toString());
		                	   }
		                	   else {
		                		   file = new File("C:\\Users\\priya\\assignmentWorkspace\\DataCenterNode\\src\\edu\\rit\\ds\\sensorData.txt");
		                	   }
		                	    fw.write(obj.toString());
		                		 
		                    }
		                }
		            }
		            return null;
		        }catch (java.io.IOException ex){
		            ex.printStackTrace();
		            return null;
		        }
		    }
		}).filter(x->x!=null ).addSink(flinkKafkaProducer);*/
		
	/*FlinkKafkaProducer09<String> flinkKafkaProducer = createStringProducer("test1", "localhost:9092");
		
		messageStream.addSink(flinkKafkaProducer);*/
		env.execute();
	}
	public FlinkKafkaProducer09<String> createStringProducer(String topic, String kafkaAddress) {
		return new FlinkKafkaProducer09<>(kafkaAddress, topic, new SimpleStringSchema());
	}
	public static void main(String args[]) throws Exception {
		FlinkConsumer consumer = new FlinkConsumer();
		consumer.transform();
		/*Map<String,String> map=new HashMap<>();
		map.put("880", "{\"id\":880,\"latitude\":41.00000000000006,\"longitude\":-122.0,\"temp\":80}");
		map.put("8801", "{\"id\":8801,\"latitude\":41.00000000000006,\"longitude\":-122.0,\"temp\":80}");
		System.out.println(consumer.arrafy(map));*/
	}
	
	/*
	public void writeToFile(String s) {
		if(fw==null) {
			try {
				fw= new FileWriter(file);
				fw.write(s);
		    	fw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			try {
				fw.write(s);
				fw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	
		}
		
	}*/

}
interface KafkaInterface {
	public boolean check(String s);
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