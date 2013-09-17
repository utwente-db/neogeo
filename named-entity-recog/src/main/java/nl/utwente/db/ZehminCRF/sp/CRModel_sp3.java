package nl.utwente.db.ZehminCRF.sp;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import nl.utwente.db.ZehminCRF.corpus.Corpus;
import nl.utwente.db.ZehminCRF.utils.Global;
import nl.utwente.db.ZehminCRF.utils.PG;
import nl.utwente.db.ZehminCRF.utils.StringInteger;

/**
 * @author Zhemin Zhu
 * Created on Oct 12, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class CRModel_sp3 {
	HashMap<String, CPT> m_CPTFF;
	HashMap<String, CPT> m_CPTF;
	HashMap<String, CPT> m_CPTOO;
	HashMap<String, CPT> m_CPTO;
	HashMap<String, PG> m_mapParameters;
	Vector<String> m_vecParameters;
	
	//sentence normalization
	private Vector<Double> m_Z;
	//sentence probability
	private Vector<Double> m_Prob;
	
	boolean m_bParaUpdated;
	double m_sigma = 1;  //hyper-parameter for L2 regularization  
	Corpus m_corpus;
	String m_modelFile;
			
	//bTraining = true: training  bTraining = false : decoding
	//corpus: training corpus
	public CRModel_sp3(Corpus corpus, String modelFile, boolean bTraining){
		if(bTraining){//training
			m_bParaUpdated = false;
			m_corpus = corpus;
			m_modelFile = modelFile;
			initParameter();
			initCPTEmp();
			calcGradients();
			saveParameters();
			System.out.println("Optimization starts ..");
		}else{ //decoding
			m_corpus = corpus;
			m_modelFile = modelFile;
			readParameters();
			initCPTPara();
			// System.out.println("Init CPTs Finished! Start to decode...");
		}
	}
	
	private void initParameter(){
		m_mapParameters = new HashMap<String, PG>();
		m_vecParameters = new Vector<String>();
		
		//init parameters for FF
		HashMap<String, CPT> CPTFF = m_corpus.constructCPTFF();
		HashMap<String, CPT> CPTFF1 = m_corpus.constructCPTFF1();
		HashMap<String, CPT> CPTFF2 = m_corpus.constructCPTFF2();
		Set<String> keys = CPTFF.keySet();
		for(String key : keys){
			CPT cptff = CPTFF.get(key);
			CPT cptff1 = CPTFF1.get(key+"1");
			CPT cptff2 = CPTFF2.get(key+"2");
			String ff = cptff.getName();
			Vector<String> ssSpace = cptff.getTagSpace();
			for(String ss : ssSpace){
				String paraName = ss + " " + ff;
				double initValue = cptff.getProb(ss) - cptff1.getProb(ss.split(" ")[0]) 
							- cptff2.getProb(ss.split(" ")[1]);
				m_mapParameters.put(paraName, new PG(initValue, 0.0));
				m_vecParameters.add(paraName);
			}
		}
		
		//init parameters for F
		HashMap<String, CPT> CPTF = m_corpus.constructCPTF();
		keys = CPTF.keySet();
		for(String key : keys){
			CPT cptF = CPTF.get(key);
			Vector<String> sSpace = cptF.getTagSpace();
			for(String s : sSpace){
				String paraName = cptF.getParaName(s);
				double initValue = cptF.getProb(s);
				m_mapParameters.put(paraName, new PG(initValue, 0.0));
				m_vecParameters.add(paraName);
			}
		}
		
		System.out.println("Parameters: " + getParameterNumber());
	}
	
	private void initCPTEmp(){
		m_CPTFF = m_corpus.constructCPTFF();
		
		Set<String> keys = m_CPTFF.keySet();
		for(String key : keys)
			m_CPTFF.get(key).m_parameters = m_mapParameters;
		
		m_CPTF = m_corpus.constructCPTF();
		keys = m_CPTF.keySet();
		for(String key : keys)
			m_CPTF.get(key).m_parameters = m_mapParameters;
		
		m_CPTO = m_corpus.constructCPTO();
		m_CPTOO = m_corpus.constructCPTOO();
	}
	
	private void initCPTPara(){
		m_CPTFF = m_corpus.constructCPTFF();
		m_CPTF = m_corpus.constructCPTF();
		m_CPTO = m_corpus.constructCPTO();
		m_CPTOO = m_corpus.constructCPTOO();
		
		Set<String> keys = m_CPTFF.keySet();
		for(String key : keys){
			m_CPTFF.get(key).updateParameters();
		}
		
		keys = m_CPTF.keySet();
		for(String key : keys){
			m_CPTF.get(key).updateParameters();
		}
		
	}
	
	HashMap<String, Double> m_mapFFZ;
	private void calcEZ(){
		m_mapFFZ = new HashMap<String, Double>();
		Vector<StringInteger> es = m_corpus.getNumFF();
		for(StringInteger si : es){
			CPT cptff = m_CPTFF.get(si.getStr());
			String ff = cptff.getName();
			String f1 = ff.split(" ")[0];
			double z = 0.0;
			Vector<String> ssSpace = cptff.getTagSpace();
			for(String ss : ssSpace){
				String s1 = ss.split(" ")[0];
				z += Math.pow(Math.E, 
						m_mapParameters.get(ss + " " + ff).getParameterValue() 
						+ m_mapParameters.get(s1 + " " + f1).getParameterValue());
			}
			m_mapFFZ.put(ff, z);
		}
	}
	
	public double calcObj(){
		double obj = 0.0;
		calcEZ();
		
		Vector<StringInteger> ssff = m_corpus.getNumSSFF();
		for(StringInteger si : ssff){
			obj += si.getInt() * m_mapParameters.get(si.getStr()).getParameterValue();
		}
		
		Vector<StringInteger> sf = m_corpus.getNumSF();
		for(StringInteger si : sf){
			obj += si.getInt() * m_mapParameters.get(si.getStr()).getParameterValue();
		}
		
		Vector<StringInteger> ff = m_corpus.getNumFF();
		for(StringInteger si : ff){
			obj -= Math.log(m_mapFFZ.get(si.getStr())) * si.getInt();
		}
		
		return obj + L2Regularization();
	}
	

	public void calcGradients(){
		double obj = 0.0;
		calcEZ();
		
		Vector<StringInteger> ssff = m_corpus.getNumSSFF();
		for(StringInteger si : ssff){
			PG para = m_mapParameters.get(si.getStr());
			para.setGradient(si.getInt() * para.getParameterValue());
		}
		
		Vector<StringInteger> sf = m_corpus.getNumSF();
		for(StringInteger si : sf){
			PG para = m_mapParameters.get(si.getStr());
			para.setGradient(para.getGradient() + si.getInt() * para.getParameterValue());
		}
		
		Vector<StringInteger> ff = m_corpus.getNumFF();
		for(StringInteger si : ff){
			CPT cptff = m_CPTFF.get(si.getStr());
			Vector<String> ssSpace = cptff.getTagSpace();
			for(String ss : ssSpace){
				PG paraSSFF = m_mapParameters.get(ss + " " + si.getStr());
				PG paraSF = m_mapParameters.get(ss.split(" ")[0] + " " 
							+ si.getStr().split(" ")[0]);
				double prob = Math.pow(Math.E, paraSSFF.getParameterValue() 
								+ paraSF.getParameterValue()) / m_mapFFZ.get(si.getStr());
				paraSSFF.setGradient(paraSSFF.getGradient() - prob);
				paraSF.setGradient(paraSF.getGradient() - prob);
			}
		}
		
	}
	
	public int getParameterNumber(){
		return m_vecParameters.size();
	}
	
	public double getGradient(int i){
		if(m_bParaUpdated) updateParameters();
		return m_mapParameters.get(m_vecParameters.get(i)).getGradient();
	}
	
	public double getParameterValue(int i){
		return m_mapParameters.get(m_vecParameters.get(i)).getParameterValue();
	}
	
	public void setParameterValue(int i, double v){
		if(m_mapParameters.get(m_vecParameters.get(i)).setParameterValue(v))
			m_bParaUpdated = true;
	}
	
	public void updateParameters(){
		if(! m_bParaUpdated) return;
		Set<String> keys = m_CPTFF.keySet();
		for(String key : keys)
			m_CPTFF.get(key).updateParameters();
		keys = m_CPTF.keySet();
		for(String key : keys)
			m_CPTF.get(key).updateParameters();
		calcGradients();
		//saveParameters();
		m_bParaUpdated = false;
	}
	
	private double L2Regularization(){
		double L2 = 0.0;
		double sigmasigma = m_sigma * m_sigma;
		for(String para : m_vecParameters){
			PG pg = m_mapParameters.get(para);
			L2 += pg.getParameterValue() * pg.getParameterValue();
			pg.addGradient(- pg.getParameterValue() / sigmasigma);
		}
		return -0.5 * L2 / sigmasigma;
	}
	
	public double getCR(String s1, String s2, Vector<String> vf1, Vector<String> vf2){
		String OO = vf1.firstElement() + " " + vf2.firstElement();
		String FF = vf1.get(1) + " " + vf2.get(1);
		String SS = s1 + " " + s2;
		if(m_CPTOO.containsKey(OO)){
			//non-OOV
			return m_CPTOO.get(OO).getProb(SS) - getProb(s1, vf1) - getProb(s2, vf2);
		}else{
			//OOV
			if(! m_mapParameters.containsKey(FF)){
				System.err.println("OOFF: " + FF);
				Double crss = m_corpus.constructEmpCRSS().get(s1 + " " + s2);
				if(crss == null) return Double.NEGATIVE_INFINITY;
				return crss;
			}
			return Math.pow(Math.E, m_mapParameters.get(FF).getParameterValue());
		}
	}
	
	public double getProb(String S, Vector<String> vf) {
		String O = vf.firstElement();
		String F = vf.get(1);
		if(m_CPTO.containsKey(O)){
			//non-OOV
			return m_CPTO.get(O).getProb(S);
		}else{
			if(! m_CPTF.containsKey(F)){
				System.err.println("OOF: " + F);
				return m_corpus.constructEmpProbS().get(S);
			}
			//OOV
			return m_CPTF.get(F).getProb(S);
		}
	}

	public Vector<String> getPromisingTags(Vector<String> vf) {
		if(m_corpus.constructCPTO().containsKey(vf.firstElement())){//non-OOV
			return m_corpus.constructCPTO().get(vf.firstElement()).getTagSpace();
		}else{//OOV
			if(! m_CPTF.containsKey(vf.get(1))){
				System.err.println("OOF: " + vf.get(1));
				m_corpus.getTagSpace();
			}
			return m_CPTF.get(vf.get(1)).getTagSpace();
		}
	}
	
	public void saveParameters(){
		try{
			  FileWriter fstream = new FileWriter(m_modelFile);
			  BufferedWriter out = new BufferedWriter(fstream);
			  int num = 0;
			  for(String para : m_vecParameters){
					PG pg = m_mapParameters.get(para);
					out.append(para + " " + pg.getParameterValue() + " " + pg.getGradient() + "\n");
					++ num;
					if(num % 100000 == 0)
						out.flush();
    		  }
			  out.flush();
			  out.close();
			  System.out.println("Save Model File: " + m_modelFile);
		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
	}
	
	private void readParameters(){
		m_mapParameters = new HashMap<String, PG>();
		String[] lines = Global.readFile(m_modelFile).split("\n");
		int num = 0;
		for(String line : lines){
			String[] strs = line.split(" ");
			StringBuilder key = new StringBuilder();
			for(int i = 0; i < strs.length - 2; ++ i)
				key.append(strs[i] + " ");
			m_mapParameters.put(key.toString().trim(), 
					new PG(Double.valueOf(strs[strs.length - 2]), Double.valueOf(strs[strs.length - 1])));
			++ num;
			if(num % 10000 == 0)
				System.out.println("Parameters Read: " + num);
		}
		System.out.println("readParameters() finished!");
	}
	
}
