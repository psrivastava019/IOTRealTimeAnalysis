package edu.ds2019.workLoadGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class WorkLoadGenerator {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		ArrayList<ArrayList<String>> grid = gridGenerate(20, 20);
		System.out.println(grid);
		HashMap<ArrayList<Integer>, HashMap<String, ArrayList<Integer>>> gridMap = generateMap(grid);
		System.out.println(gridMap);
		traverseFire(new ArrayList<Integer>(Arrays.asList(10,10)), "E", gridMap);
	}

	private static void traverseFire(ArrayList<Integer> startCoord, String directn, HashMap<ArrayList<Integer>, HashMap<String, ArrayList<Integer>>> gridMap) throws InterruptedException {
		
		if( !gridMap.containsKey(startCoord)) {
			
			if(gridMap.containsKey(Arrays.asList(startCoord.get(0)+1, startCoord.get(1)))) {
				startCoord.set(0, startCoord.get(0)+1);
			}
			if(gridMap.containsKey(Arrays.asList(startCoord.get(0)-1, startCoord.get(1)))) {
				startCoord.set(0, startCoord.get(0)-1);
			}
			if(gridMap.containsKey(Arrays.asList(startCoord.get(0), startCoord.get(1)+1))) {
				startCoord.set(1, startCoord.get(1)+1);
			}
			if(gridMap.containsKey(Arrays.asList(startCoord.get(0), startCoord.get(1)-1))) {
				startCoord.set(1, startCoord.get(1)-1);
			}
		}
		ForestFireProducer produceFire = new ForestFireProducer(startCoord);
		new Thread(produceFire).start();
		Thread.sleep(5000);
		HashMap<ArrayList<Integer>, Boolean> visited = new HashMap<ArrayList<Integer>, Boolean>();
		visited.put(new ArrayList<Integer>( Arrays.asList(startCoord.get(0),startCoord.get(1))), true);
		ArrayList<ArrayList<Integer>> queue = new ArrayList<ArrayList<Integer>>();
		queue.add(new ArrayList<Integer>(Arrays.asList(startCoord.get(0),startCoord.get(1))));
		while(queue.size() > 0) {
			ArrayList<Integer> node = queue.remove(queue.size()-1);
			ArrayList<Integer> node1 = new ArrayList<Integer>();
			ArrayList<Integer> node2 = new ArrayList<Integer>();
			ArrayList<Integer> node3 = new ArrayList<Integer>();
			if(gridMap.containsKey(node) && gridMap.get(node).containsKey(directn)) {
				
				if(directn.equals("N")) {
					node1 = gridMap.get(node).get("N");
					if(gridMap.get(node).containsKey("NE")) {
						node2 = gridMap.get(node).get("NE");
					}
					if(gridMap.get(node).containsKey("NW")) {
						node3 = gridMap.get(node).get("NW");
					}
				}
				if(directn.equals("NE")) {
					node1 = gridMap.get(node).get("NE");
					if(gridMap.get(node).containsKey("N")) {
						node2 = gridMap.get(node).get("N");
					}
					if(gridMap.get(node).containsKey("E")) {
						node3 = gridMap.get(node).get("E");
					}
				}
				if(directn.equals("E")) {
					node1 = gridMap.get(node).get("E");
					if(gridMap.get(node).containsKey("NE")) {
						node2 = gridMap.get(node).get("NE");
					}
					if(gridMap.get(node).containsKey("SE")) {
						node3 = gridMap.get(node).get("SE");
					}
				}
				if(directn.equals("SE")) {
					node1 = gridMap.get(node).get("SE");
					if(gridMap.get(node).containsKey("E")) {
						node2 = gridMap.get(node).get("E");
					}
					if(gridMap.get(node).containsKey("S")) {
						node3 = gridMap.get(node).get("S");
					}
				}
				if(directn.equals("S")) {
					node1 = gridMap.get(node).get("S");
					if(gridMap.get(node).containsKey("SE")) {
						node2 = gridMap.get(node).get("SE");
					}
					if(gridMap.get(node).containsKey("SW")) {
						node3 = gridMap.get(node).get("SW");
					}
				}
				if(directn.equals("SW")) {
					node1 = gridMap.get(node).get("SW");
					if(gridMap.get(node).containsKey("S")) {
						node2 = gridMap.get(node).get("S");
					}
					if(gridMap.get(node).containsKey("W")) {
						node3 = gridMap.get(node).get("W");
					}
				}
				if(directn.equals("W")) {
					node1 = gridMap.get(node).get("W");
					if(gridMap.get(node).containsKey("SW")) {
						node2 = gridMap.get(node).get("SW");
					}
					if(gridMap.get(node).containsKey("NW")) {
						node3 = gridMap.get(node).get("NW");
					}
				}
				if(directn.equals("NW")) {
					node1 = gridMap.get(node).get("NW");
					if(gridMap.get(node).containsKey("W")) {
						node2 = gridMap.get(node).get("W");
					}
					if(gridMap.get(node).containsKey("N")) {
						node3 = gridMap.get(node).get("N");
					}
				}
				
				if(node1.size() > 0 && !visited.containsKey(node1)) {
					visited.put(node1, true);
					queue.add(node1);
					produceFire = new ForestFireProducer(node1);
					new Thread(produceFire).start();
				}
				if(node2.size() > 0 && !visited.containsKey(node2)) {
					visited.put(node2, true);
					queue.add(node2);
					produceFire = new ForestFireProducer(node2);
					new Thread(produceFire).start();
				}
				if(node3.size() > 0 && !visited.containsKey(node3)) {
					visited.put(node3, true);
					queue.add(node3);
					produceFire = new ForestFireProducer(node2);
					new Thread(produceFire).start();
				}
				Thread.sleep(5000);
			}
		}
	}
	
	private static HashMap<ArrayList<Integer>, HashMap<String, ArrayList<Integer>>> generateMap(ArrayList<ArrayList<String>> grid){
		HashMap<ArrayList<Integer>, HashMap<String, ArrayList<Integer>>> gridMap = new HashMap<ArrayList<Integer>, HashMap<String,ArrayList<Integer>>>();
		for(int row=0; row < grid.size(); row++) {
			for(int col=0; col < grid.get(row).size(); col++) {
				if(grid.get(row).get(col).equals("1")) {
					if(!gridMap.containsKey(new ArrayList<Integer>(Arrays.asList(row,col)))) {
						gridMap.put(new ArrayList<Integer>(Arrays.asList(row,col)), new HashMap<String, ArrayList<Integer>>());
					}
					if((row - 2) > 0) {
						gridMap.get(new ArrayList<Integer>(Arrays.asList(row, col))).put("N", new ArrayList<Integer>(Arrays.asList(row-2, col)));
					}
					if((row - 1) > 0 && (col + 1) < grid.get(row).size()) {
						gridMap.get(new ArrayList<Integer>(Arrays.asList(row, col))).put("NE", new ArrayList<Integer>(Arrays.asList(row-1, col+1)));
					}
					if((col + 2) < grid.get(row).size()) {
						gridMap.get(new ArrayList<Integer>(Arrays.asList(row, col))).put("E", new ArrayList<Integer>(Arrays.asList(row, col+2)));
					}
					if((col+1) < grid.get(row).size() && (row+1)<grid.size()) {
						gridMap.get(new ArrayList<Integer>(Arrays.asList(row, col))).put("SE", new ArrayList<Integer>(Arrays.asList(row+1, col+1)));
					}
					if((row + 2) < grid.size()) {
						gridMap.get(new ArrayList<Integer>(Arrays.asList(row, col))).put("S", new ArrayList<Integer>(Arrays.asList(row+2, col)));
					}
					if((row + 1) < grid.size() && (col-1) > 0) {
						gridMap.get(new ArrayList<Integer>(Arrays.asList(row, col))).put("SW", new ArrayList<Integer>(Arrays.asList(row+1, col-1)));
					}
					if((col - 2) > 0) {
						gridMap.get(new ArrayList<Integer>(Arrays.asList(row, col))).put("W", new ArrayList<Integer>(Arrays.asList(row, col-2)));
					}
					if((row - 1) > 0 && (col-1)>0) {
						gridMap.get(new ArrayList<Integer>(Arrays.asList(row, col))).put("NW", new ArrayList<Integer>(Arrays.asList(row-1, col-1)));
					}
				}
			}
		}
		return gridMap;
	}
	private static ArrayList<ArrayList<String>> gridGenerate(int rows, int columns){
		ArrayList<ArrayList<String>> grid = new ArrayList<ArrayList<String>>();
		String data = "0";
		for(int row=0; row < rows; row++) {
			ArrayList<String> subGrid = new ArrayList<String>();
			for(int col=0; col < columns; col++) {
				subGrid.add(data);
				if(data.equalsIgnoreCase("0")) {
					data = "1";
				}
				else {
					data = "0";
				}
			}
			grid.add(subGrid);
			if(data.equalsIgnoreCase("0")) {
				data = "1";
			}
			else {
				data = "0";
			}
		}
		return grid;
		
	}
}
