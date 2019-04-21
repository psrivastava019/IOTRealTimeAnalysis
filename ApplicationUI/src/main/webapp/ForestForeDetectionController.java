package main.webapp;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Servlet implementation class ForestForeDetectionController
 */
@WebServlet("/ForestForeDetectionController")
public class ForestForeDetectionController extends HttpServlet {
	private static final long serialVersionUID = 1L;
	//private static String path = "C:\\Users\\kamat\\Desktop\\DS\\project";
	ConsumerKafka cf=new ConsumerKafka();
	boolean flag=false;
	final Consumer<Long, String> consumer=cf.createConsumer();
    /**
     * Default constructor. 
     */
    public ForestForeDetectionController() {
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println("ForestForeDetectionControllerForestForeDetectionControllerForestForeDetectionController");
		response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Pragma", "no-cache");
        PrintWriter out = response.getWriter();
		/*DataStream<String> stream=null;
		try {
			if(flag==false) {
				stream = cf.transform();
				flag=true;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
        
        try {
			ConsumerRecords<Long, String> cr= cf.runConsumer(consumer);
//			ArrayList<String> data = new ArrayList<>();
			/*for(ConsumerRecord<Long, String> c:cr) {
				System.out.println(c.value());
				out.write(c.value());
//				data.add(c.value());
				System.out.println(" gh");
				//break;
			}*/
//			if(cr.count() != 0) {
//				out.write(data.get(data.size()-1));
//			}
			
		Iterator<ConsumerRecord<Long, String>> it=cr.iterator();
			for(int i=1;i<=cr.count();i++) {
				if(i==cr.count()) {
					out.write(it.next().value());
				}
				Thread.sleep(1000);
				/*else {
					out.write(it.next().value()+",");
				}*/
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        
        
        /*if(stream!=null) {
        	System.out.println("IN CONTROLLER");
        	out.write(stream.map(new MapFunction<String, String>() {
    			@Override
    		    
    		    public String map(String value) throws Exception {
    		    	
    		        ObjectMapper mapper = new ObjectMapper();
    		        try {
    		        	JsonNode rootNode = mapper.readValue(value, JsonNode.class);
    		            if (rootNode.isObject()) {
    		            	ObjectNode obj = mapper.convertValue(rootNode, ObjectNode.class);
    		                if (obj.has("temp")) {
    		                    	return obj.toString();
    		                }
    		            }
    		            return null;
    		        }catch (java.io.IOException ex){
    		            ex.printStackTrace();
    		            return null;
    		        }
    		    }
    		}).filter(x->x!=null).toString());
        }
        else {
        	System.out.println("hshk");
        }
        */

       /* BufferedReader read = new BufferedReader(new FileReader(new File(path+"\\cities.json")));
        String line = "";
        String data = "";
        while((line = read.readLine()) != null) {
        	data+=line;
        }
        read.close();
        out.write(data);*/
        out.close();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}
}
