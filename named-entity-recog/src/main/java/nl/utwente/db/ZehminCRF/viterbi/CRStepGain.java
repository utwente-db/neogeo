package nl.utwente.db.ZehminCRF.viterbi;

import nl.utwente.db.ZehminCRF.corpus.Sentence;
import nl.utwente.db.ZehminCRF.sp.CRModel_sp1;

/**
 * @author Zhemin Zhu
 * Created on Sep 4, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class CRStepGain implements StepGain
{
	protected CRModel_sp1 m_model;
	
	public CRStepGain(CRModel_sp1 pm) {
		m_model = pm;
	}

	//the first gain (i = 1): p(s1|o1)
	public double gain0(Sentence s, String tag) {
		return m_model.getProb(tag, s.getColumn(0));
	}
	
	//gain for i = 2...n: p(si-1, si | oi-1, oi) / p(si-1 | oi-1)
	public double gain(Sentence s, int i, String preTag, String curTag) {
		String preS = preTag;
		String curS = curTag;
		String preO = s.getWord(i - 1);
		String curO = s.getWord(i);
		return m_model.getCR(preS, curS, s.getColumn(i-1), s.getColumn(i)) 
				+ m_model.getProb(curS, s.getColumn(i));
	}
	
}
