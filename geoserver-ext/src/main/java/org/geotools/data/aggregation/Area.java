package org.geotools.data.aggregation;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;

public class Area {

	private double lowX, highX, lowY, highY;
//	private boolean set = false;

//	public Area(){
//	}

	public Area(double lowX, double highX, double lowY, double highY){
		this.lowX = lowX;
		this.highX = highX;
		this.lowY = lowY;
		this.highY = highY;
//		set = true;
	}

	/**
	 * parsing a polygon literal of the form:
	 * POLYGON ((-0.1516689453125 51.308060546875005, -0.1516689453125 51.677939453125, 0.4806689453125 51.677939453125, 0.4806689453125 51.308060546875005, -0.1516689453125 51.308060546875005) 
	 * @param poly
	 */
	public static Area parsePolygon(Polygon poly){
		Coordinate[] points = poly.getCoordinates();
		double lowX,lowY,highX,highY;
		if(points.length!=5) return null;
		lowX = points[0].x;
		highX = lowX; 
		lowY = points[0].y;
		highY = lowY;
		for(int i=1;i<points.length;i++){
			double curX = points[i].x;
			double curY = points[i].y;
			lowX = curX<lowX ? curX : lowX;
			highX = curX>highX ? curX : highX;
			lowY = curY<lowY ? curY : lowY;
			highY = curY>highY ? curY : highY;
		}
		return new Area(lowX,highX,lowY,highY);
	}
	
	public static Area parsePolygon(String poly){
		poly = poly.trim();
		if(!poly.startsWith("POLYGON")) return null;
		poly = poly.replaceAll("(", "").replaceAll(")","").trim();
		String[] points = poly.split(",");
		if(points.length!=5) return null;
		double lowX,lowY,highX,highY;
		String[] pos = points[0].split(" ");
		if(pos.length!=2) return null;
		lowX = Double.valueOf(pos[0]);
		highX = lowX; 
		lowY = Double.valueOf(pos[1]);
		highY = lowY;
		for(int i=1;i<points.length;i++){
			pos = points[0].split(" ");
			if(pos.length!=2) return null;
			double curX = Double.valueOf(pos[0]);
			double curY = Double.valueOf(pos[1]);
			lowX = curX<lowX ? curX : lowX;
			highX = curX>highX ? curX : highX;
			lowY = curY<lowY ? curY : lowY;
			highY = curY>highY ? curY : highY;
		}
		return new Area(lowX,highX,lowY,highY);
	}

	public double getLowX() {
		return lowX;
	}

	public double getHighX() {
		return highX;
	}

	public double getLowY() {
		return lowY;
	}

	public double getHighY() {
		return highY;
	}

	public void updateBounds(double lowX, double highX, double lowY, double highY){
		this.lowX = this.lowX < lowX ? lowX : this.lowX;
		this.highX = this.highX > highX ? highX : this.highX;
		this.lowY = this.lowY < lowY ? lowY : this.lowY;
		this.highY = this.highY > highY ? highY : this.highY;	
	}

	public void updateBounds(Area a){
		if(a!=null)
			updateBounds(a.getLowX(), a.getHighX(), a.getLowY(), a.getLowY());	
	}

//	public boolean isValid(){
//		return set;
//	}

	@Override
	public String toString(){
		return "area("+lowX+","+highX+","+lowY+","+highY+")";
	}
}
