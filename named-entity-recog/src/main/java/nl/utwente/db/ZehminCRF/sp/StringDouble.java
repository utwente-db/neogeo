package nl.utwente.db.ZehminCRF.sp;

/**
 * @author Zhemin Zhu
 * Created on Oct 16, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class StringDouble {
	public String m_str;
	public double m_d;
	public StringDouble(String str, double d){
		m_str = str;
		m_d = d;
	}
	public void add(double diff){
		m_d += diff;
	}
}