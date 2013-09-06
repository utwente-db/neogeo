package nl.utwente.db.ZehminCRF.sp;

import java.util.HashMap;
import java.util.Vector;

import nl.utwente.db.ZehminCRF.utils.PG;

/**
 * @author Zhemin Zhu
 * Created on Oct 12, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class CPT {
	String m_f;
	HashMap<String, Double> m_mapCNTs;
	Vector<String> m_tagSpace;
	double m_sumCNT;
	HashMap<String, PG> m_parameters;
	
	public CPT(String f, Vector<String> tags, HashMap<String, PG> parameters){
		m_f = f;
		m_mapCNTs = new HashMap<String, Double>();
		m_sumCNT = 0.0;
		m_parameters = parameters;
		m_tagSpace = tags;
		for(int i = 0; i < m_tagSpace.size(); ++ i)
			addTag(m_tagSpace.get(i));
	}
	
	public CPT(String f){
		m_f = f;
		m_mapCNTs = new HashMap<String, Double>();
		m_sumCNT = 0.0;
		m_parameters = null;
		m_tagSpace = new Vector<String>();
	}
	
	public String getName(){
		return m_f;
	}
	
	public String getParaName(int i){
		return getParaName(m_tagSpace.get(i));
	}
	
	public String getParaName(String tag){
		//return m_f + " " + tag;
		return tag + " " + m_f;
	}
	
	public int getNumTag(){
		return m_tagSpace.size();
	}
	
	public String getTag(int i){
		return m_tagSpace.get(i);
	}
	

	
	public double getProbNonLog(String s){
		Double cntS = m_mapCNTs.get(s);
		if(cntS != null){
			return cntS / m_sumCNT;
		}else{
			return 0.0;
		}
	}
	
	
	//return log prob
	public double getProb(String s){
		Double cntS = m_mapCNTs.get(s);
		if(cntS != null){
			return Math.log(cntS / m_sumCNT);
		}else{
			return Double.NEGATIVE_INFINITY;
		}
	}
	
	public void print(){
		StringBuilder sb = new StringBuilder();
		Vector<String> tags = getTagSpace();
		for(String tag : tags){
			sb.append(tag + " " + m_mapCNTs.get(tag) + "\n");
		}
		System.out.println(sb.toString());
	}
	
	public Vector<String> getTagSpace(){
		return m_tagSpace;
	}
	
	public void addTag(String t, double cnt){
		if(m_mapCNTs.containsKey(t)){
			m_mapCNTs.put(t, m_mapCNTs.get(t) + cnt);
		}else{
			m_mapCNTs.put(t, cnt);
			m_tagSpace.add(t);
		}
		m_sumCNT += cnt;
	}
	
	public void addTag(String s) {
		double cnt = calcEstimatedCNT(s);
		m_mapCNTs.put(s, cnt);
		m_sumCNT += cnt;
	}
	
	public double calcEstimatedCNT(String tag){
		double cnt = 0.0;
		cnt = m_parameters.get(getParaName(tag)).getParameterValue();
		return Math.pow(Math.E, cnt);
	}
	
	public void updateParameters(){
		m_sumCNT = 0.0;
		for(String tag : m_tagSpace){
			double cnt = calcEstimatedCNT(tag);
			m_mapCNTs.put(tag, cnt);
			m_sumCNT += cnt;
		}
	}
	
	public void calcGradientsZ(double cnt){
		Vector<String> tagSpace = getTagSpace();
		for(String tag : tagSpace){
			double diff = - cnt * m_mapCNTs.get(tag) / m_sumCNT;
			PG pg = m_parameters.get(getParaName(tag));
			pg.addGradient(diff);
		}
	}
	
	public void calcGradientsS(String tag, double cnt){
		PG pg = m_parameters.get(getParaName(tag));
		pg.addGradient(cnt);
	}
}
