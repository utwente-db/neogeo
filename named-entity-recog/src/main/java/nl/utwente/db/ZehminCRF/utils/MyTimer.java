package nl.utwente.db.ZehminCRF.utils;

/**
 * @author Zhemin Zhu
 * Created on Jul 30, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class MyTimer {
	private long m_start;
	private long m_end;
	
	public void start(){
		m_start = System.nanoTime();	
	}
	
	public void end(){
		m_end = System.nanoTime();	
	}
	
	public double getTime(){
		return ((double) (m_end - m_start)) /1000000000; 
	}
	
	public String toString(){
		return "Time(sec): " + getTime();
	}
	
	public void printTime(String message){
		System.out.println(message + ": " + toString());
	}
	
	public void printTime(){
		System.out.println(toString());
	}
	
	public void reset(){
		m_start = -1;
		m_end = -1;
	}
	
}
