package org.mc2.kmeans;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class Point implements Writable{

	private double x;
	
	private double y;
	
	public Point(double x, double y) {
		this.x = x;
		this.y = y;
	}
	
	public Point() {
		this.x = 0.0;
		this.y = 0.0;
	}

	public void setPosition(Point p) {
		this.x = p.x;
		this.y = p.y;
	}
	
	public void setPosition(String pos) throws FormatIllegalException{
		this.setPosition(pos, ",");
	}
	
	public void setPosition(String pos, String split) throws FormatIllegalException{
		String[] position = pos.split(split);
		if (position.length != 2) {
			throw new FormatIllegalException();
		}
		else {
			this.x = Double.parseDouble(position[0]);
			this.y = Double.parseDouble(position[1]);
		}
	}
	
	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}
	
	
	public boolean equals(Point p) {
		if (this == p) {
			return true;
		}
		if (this.x != p.x) {
			return false;
		}
		if (this.y != p.y) {
			return false;
		}
		return true;
	}
	
	public double distance(Point p) {
		return Math.sqrt(Math.pow(this.x - p.x, 2) + Math.pow(this.y - p.y, 2));
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeDouble(x);
		out.writeDouble(y);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.x = in.readDouble();
		this.y = in.readDouble();
		
	}
	
	public String toString() {
	    String result = "";
	    result = result + this.x + "," + this.y;
	    return result;
	}
	
}
