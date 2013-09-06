package nl.utwente.db.ZehminCRF.utils;

/**
 * @author Zhemin Zhu
 * Created on Sep 3, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class PG {
	double m_parameterValue;
	double m_gradient;
	public PG(double parameter, double gradient){
		m_parameterValue = parameter;
		m_gradient = gradient;
	}
	public PG(){
		m_parameterValue = 0.0;
		m_gradient = 0.0;
	}
	public PG(double value){
		m_parameterValue = value;
		m_gradient = 0.0;
	}
	public double getParameterValue(){
		return m_parameterValue;
	}
	public double getGradient(){
		return m_gradient;
	}
	public boolean setParameterValue(double v){
		if(v == m_parameterValue)
			return false;
		m_parameterValue = v;
		return true;
	}
	public void setGradient(double g){
		m_gradient = g;
	}
	public void addGradient(double deltaG){
		m_gradient += deltaG;
	}
	public void reset(){
		m_parameterValue = Double.NEGATIVE_INFINITY;
		m_gradient = 0.0;
	}
}
