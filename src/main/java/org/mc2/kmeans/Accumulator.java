package org.mc2.kmeans;

public class Accumulator {

	private double x;
	private double y;
	private int num;
	
	public Accumulator() {
		this.x = 0.0;
		this.y = 0.0;
		this.num = 0;
	}
	
	public void add (Point point) {
		this.x += point.getX();
		this.y += point.getY();
		this.num ++;
	}
	
	public Point getCenter() {
		Point point = new Point();
		if (this.num != 0) {
			point.setX(this.x/this.num);
			point.setY(this.y/this.num);
		}
		else {
			point.setX(0.0);
			point.setY(0.0);
		}
		
		return point;
	}
	
	public void clear() {
		this.x = 0.0;
		this.y = 0.0;
		this.num = 0;
	}
	
}
