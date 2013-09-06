package nl.utwente.db.ZehminCRF.sp;

import java.util.Vector;


/**
 * @author Zhemin Zhu
 * Created on Oct 12, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class Feature {
	String m_name;
	Vector<StringDouble> m_tags;
	double m_sumCNT;
	
	public Feature(String name){
		m_name = name;
		m_tags = new Vector<StringDouble>();
		m_sumCNT = 0;
	}
	
	public void addTag(String tag, double diff){
		//existing
		for(StringDouble si : m_tags){
			if(si.m_str.equals(tag)){
				si.add(diff);
				++ m_sumCNT;
				return;
			}
		}
		//new tag
		m_tags.add(new StringDouble(tag, 1));
		++ m_sumCNT;
	}
	
	public Vector<String> getTags(){
		Vector<String> tags = new Vector<String>();
		for(StringDouble si : m_tags){
			tags.add(si.m_str);
		}
		return tags;
	}
	
	public Vector<StringDouble> getCNTTags(){
		return m_tags;
	}
	
	public int getNumTag(){
		return  m_tags.size();
	}
	
	public String getName(){
		return m_name;
	}
	
	public StringDouble getTag(int i){
		return m_tags.get(i);
	}
	
	public double getCNTSum(){
		return m_sumCNT;
	}
	
	public String getParaName(int i){
		return m_name + " " + m_tags.get(i).m_str;
	}
	
	public double getParaEmpricalProb(int i){
		return Math.log(m_tags.get(i).m_d / m_sumCNT);
	}
}
