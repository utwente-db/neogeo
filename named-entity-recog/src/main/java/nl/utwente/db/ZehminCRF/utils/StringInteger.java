package nl.utwente.db.ZehminCRF.utils;

/**
 * @author Zhemin Zhu
 * Created on Sep 8, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class StringInteger {
	String m_str;
	int m_int;
	public StringInteger(String str, int i){
		m_str = str;
		m_int = i;
	}
	
	public int getInt(){
		return m_int;
	}
	
	public String getStr(){
		return m_str;
	}
}
