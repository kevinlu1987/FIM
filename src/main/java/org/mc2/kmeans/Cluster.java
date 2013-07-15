package org.mc2.kmeans;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Cluster {

	private Map<Point, List<Point>> cluster;
	private List<Point> observations;
	
	private InputStream is;
	private BufferedReader br;
	String line;
	
	public Cluster() {
		cluster = new HashMap<Point, List<Point>>();
		observations = new LinkedList<Point>();
	}
	
	public void init(String clusterPath, String observationPath) throws FileNotFoundException {
		readCluster(clusterPath);
		readObservation(observationPath);
	}
	

	public void cluster(int round) {
		for (int i=0; i<round; i++) {
			this.oneRoundClustering();
		}
	}
	
	public void oneRoundClustering() {
		
		// find out which point is the closest from point and add the point to the hash map.
		Iterator observationIterator = this.observations.iterator();
		
		this.cleanClusterObservations();
		
		while (observationIterator.hasNext()) {
			Point observationPoint = (Point)observationIterator.next();
			Iterator clusterIterator = this.cluster.keySet().iterator();
			
			double minLength = Double.MAX_VALUE;
			Point minPoint = null;
			Point clusterPoint = null;
			double distance = 0.0;
			
			while(clusterIterator.hasNext()) {
				clusterPoint = (Point)clusterIterator.next();
				distance = clusterPoint.distance(observationPoint);
				if (distance < minLength) {
					minLength = distance;
					minPoint = clusterPoint;
				}
			}
			
			if (minPoint != null) {
				cluster.get(minPoint).add(observationPoint);
				
//				System.out.println("minLength = " + minLength);
//				System.out.print("c(" + minPoint.getX() + ","+minPoint.getY() + ")  ");
//				System.out.println("o(" + observationPoint.getX() + "," + observationPoint.getY()+")");
//				System.out.println();
			}
		}
		

		
		Iterator clusterIterator = this.cluster.entrySet().iterator();
		Accumulator accumulator = new Accumulator();
		while (clusterIterator.hasNext()) {
			Map.Entry<Point, List<Point>> entry = (Map.Entry<Point, List<Point>>)clusterIterator.next();
			
			Point newPoint = entry.getKey();
			List list = entry.getValue();
			Iterator obstns = list.iterator();
			
//			System.out.println("key : x=" + newPoint.getX() + ", y=" + newPoint.getY());
			
			accumulator.clear();
			while (obstns.hasNext()) {
				Point p = (Point)obstns.next();
				accumulator.add(p);
			}
			
			newPoint.setPosition(accumulator.getCenter());			
		}
		
	}
	
	
	private void cleanClusterObservations() {
		Iterator<List<Point>> it = this.cluster.values().iterator();
		while (it.hasNext()) {
			List<Point> list = it.next();
			list.clear();
		}
	}
	

	// read observations from file into this.observations list.
	private void readObservation(String observationPath) throws FileNotFoundException {
		is = new FileInputStream(observationPath);
		br = new BufferedReader(new InputStreamReader(is));
		try {
			while ((line = br.readLine()) != null) {
				String[] splits = line.split(":");
				
				for (int i=0; i<splits.length; i++) {
					String[] num = splits[i].split(",");
					try {
						int num0 = Integer.parseInt(num[0].trim());
						int num1 = Integer.parseInt(num[1].trim());
						Point point = new Point( num0 , num1 );
						this.observations.add(point);
					}
					catch (ArrayIndexOutOfBoundsException e) {
						break;
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void readCluster(String path) throws FileNotFoundException {
		is = new FileInputStream(path);
		br = new BufferedReader(new InputStreamReader(is));
		try {
			while ((line = br.readLine()) != null) {
				String[] splits = line.split(":");
				
				for (int i=0; i<splits.length; i++) {
					String[] num = splits[i].split(",");
					try {
						int num0 = Integer.parseInt(num[0].trim());
						int num1 = Integer.parseInt(num[1].trim());
						Point point = new Point( num0 , num1 );	
						this.cluster.put(point, new ArrayList<Point>());
					}
					catch (ArrayIndexOutOfBoundsException e) {
						break;
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void printCluster() {
		System.out.println("---------------------------Clusters-------------------------------");
		Iterator<Point> it = this.cluster.keySet().iterator();
		while (it.hasNext()) {
			Point p = (Point)it.next();
			System.out.print(""+ p.getX()+","+p.getY()+" : ");
		}
		System.out.println();
	}
	
	public void printObservations() {
		Iterator<Point> it = this.observations.iterator();
		System.out.println("---------------------------Observations-------------------------------");
		while (it.hasNext()) {
			Point p = (Point)it.next();
			System.out.print(""+ p.getX()+","+p.getY()+" : ");
		}
		System.out.println();
	}
	
	public void printAllInfo() {
		Iterator clusterIterator = this.cluster.entrySet().iterator();
		while (clusterIterator.hasNext()) {
			Map.Entry<Point, List<Point>> entry = (Map.Entry<Point, List<Point>>)clusterIterator.next();
			
			Point point = entry.getKey();
			List list = entry.getValue();
			Iterator obstns = list.iterator();
			
			System.out.println("key : x=" + point.getX() + ", y=" + point.getY());
			System.out.println("observations belong to the key:");
			while (obstns.hasNext()) {
				Point p = (Point)obstns.next();
				System.out.print("("+p.getX()+") ");
			}
		}
	}
	
	public static void main(String[] args) {
		Cluster cluster = new Cluster();
		try {
			cluster.init("/home/lukuen/kmeans/cluster.txt", "/home/lukuen/kmeans/observations.txt");
			cluster.cluster(2);
			cluster.printAllInfo();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
