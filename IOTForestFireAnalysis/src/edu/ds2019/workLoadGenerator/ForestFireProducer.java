package edu.ds2019.workLoadGenerator;
import java.util.ArrayList;

public class ForestFireProducer implements Runnable {

	private ArrayList<Integer> coordinates;
	public ForestFireProducer(ArrayList<Integer> coordinates) {
		this.coordinates = coordinates;
	}
	
	@Override
	public void run() {
		while(true) {
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