package edu.ds2019.workLoadGenerator;

import java.io.Serializable;
import java.util.ArrayList;

public class IOTDeviceMaster implements Serializable {
	 
	private ArrayList<IOTDevice> iotData;

	public ArrayList<IOTDevice> getIotData() {
		return iotData;
	}

	public void setIotData(ArrayList<IOTDevice> iotData) {
		this.iotData = iotData;
	}
	
	
}
