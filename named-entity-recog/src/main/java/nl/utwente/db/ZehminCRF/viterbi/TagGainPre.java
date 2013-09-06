package nl.utwente.db.ZehminCRF.viterbi;

/**
 * @author Zhemin Zhu
 * Created on Aug 25, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class TagGainPre{
	public String m_tag;
	public double m_gain;
	public int m_pre;
	
	public TagGainPre(String tag, double gain, int pre){
		m_tag = tag;
		m_gain = gain;
		m_pre = pre;
	}
	
	public void print(){
		System.out.println(m_tag + " " + m_gain + " " + m_pre);
	}
	
	public double getGain(){
		return m_gain;
	}
	
	public int getPre(){
		return m_pre;
	}
	
	public String getTag(){
		return m_tag;
	}
	
	public void setGain(double gain){
		m_gain = gain;
	}
	
	public void setPre(int index){
		m_pre = index;
	}
	
	public String toString(){
		return m_tag + " " + m_gain + " " + m_pre;
	}
	
}

