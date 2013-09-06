package nl.utwente.db.ZehminCRF.utils;

/**
 * @author Zhemin Zhu
 * Created on Sep 18, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class StringDouble {
	String m_str;
	Double m_double;
	public StringDouble(String str, double d){
		m_str = str;
		m_double = d;
	}
	
	public String getStr(){
		return m_str;
	}
	
	public double getDouble(){
		return m_double;
	}
	
	public void setStr(String str){
		m_str = str;
	}
	
	public void setDouble(double d){
		m_double = d;
	}

}

