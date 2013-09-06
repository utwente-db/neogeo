package nl.utwente.db.ZehminCRF.sp;

import nl.utwente.db.ZehminCRF.corpus.Corpus;
import nl.utwente.db.ZehminCRF.utils.MyTimer;
import cc.mallet.optimize.Optimizable;


public class OptimizerPMI implements Optimizable.ByGradientValue {
		CRModel_sp3 model;
		int iter = 0;
		int m_numParameters;
		double m_objValue;
		double[] m_gradient;
		public MyTimer m_timer = new MyTimer();
		private Corpus m_trainingCorpus;
	 	
	    public OptimizerPMI(Corpus trainingCorpus, String modelFile) {
	    	m_trainingCorpus = trainingCorpus;
			model = new CRModel_sp3(m_trainingCorpus, modelFile, true);
	    	m_numParameters = model.getParameterNumber();
	    	m_timer.start();
	    	m_objValue = model.calcObj();
	    	m_gradient = new double[m_numParameters];
	    	for(int i = 0; i < m_numParameters; ++ i){
	    		m_gradient[i] = model.getGradient(i);
	    	}
	    }
	    
	    public void saveParameters(){
	    	model.saveParameters();
	    }

	    public double getValue() {
    		System.out.println("Obj: " + m_objValue + " Iter" + iter);
    		return m_objValue;
	    }

	    public void getValueGradient(double[] gradient) {
	    	for(int i = 0; i < m_gradient.length; ++ i)
	    		gradient[i] = m_gradient[i];
	    }

	    // The following get/set methods satisfy the Optimizable interface

	    public int getNumParameters() { 
	    	return m_numParameters;
	    }
	    
	    public double getParameter(int i) { 
	    	return model.getParameterValue(i);
	    }
	    
	    public void getParameters(double[] buffer) {
	        for(int i = 0; i < m_numParameters; ++ i){
	        	buffer[i] = model.getParameterValue(i);
	        }
	    }
	    
	    public void setParameter(int i, double r) {
	    	System.out.println("New Parameter" + i + " " + r);
	    	model.setParameterValue(i, r);
	    	m_objValue = model.calcObj();
	    	for(int k = 0; k < m_numParameters; ++ k){
	    		m_gradient[k] = model.getGradient(k);
	    	}
	    	++ iter;
	    }
	    
	    public void setParameters(double[] newParameters) {
	    	System.out.println("New Parameter0: " + newParameters[0]);
	    	MyTimer timer = new MyTimer();
	    	timer.start();
	    	for(int i = 0; i < m_numParameters; ++ i){
	        	model.setParameterValue(i, newParameters[i]);
	        }
	    	m_objValue = model.calcObj();
	    	for(int i = 0; i < m_numParameters; ++ i){
	    		m_gradient[i] = model.getGradient(i);
	    	}
	    	timer.end();
	    	timer.printTime("Iter");
	    	++ iter;
	    }
	    
}
