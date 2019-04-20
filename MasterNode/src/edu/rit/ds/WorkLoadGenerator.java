package edu.rit.ds;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;

public class WorkLoadGenerator {
	public static void main(String args[]) throws IOException {
		String direction = "";
		int start = 0;
		int end = 0;
		if (args.length != 0) {
			start = Integer.parseInt(args[0]);
			end = Integer.parseInt(args[1]);
			direction = args[2];
		}
		while (true) {
			JSONObject obj = createJson(start, end, direction);
			StringWriter out = new StringWriter();
			obj.writeJSONString(out);
			String jsonText = out.toString();
			kafkaProducer(jsonText);
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static JSONObject createJson(int start, int end, String direction) {

		JSONObject obj = new JSONObject();
		JSONObject obj1 = new JSONObject();

		Random r = new Random();

		int result = r.nextInt(end - start) + start;

		obj.put("temp", new Integer(result));
		obj.put("Wind Direction", direction);
		obj.put("Sensor Id", new Integer(1001));
//		obj.put("Data", obj1);
		obj.put("longitude", new Double(13.0412658));
		obj.put("latitude", new Double(13.0412658));
		return obj;

	}

	public static void kafkaProducer(String jsonString) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		KafkaProducer kafkaProducer = new KafkaProducer(properties);
		try {

			kafkaProducer.send(new ProducerRecord("test", jsonString));

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			kafkaProducer.close();
		}
	}
}
