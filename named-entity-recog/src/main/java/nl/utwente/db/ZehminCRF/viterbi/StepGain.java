package nl.utwente.db.ZehminCRF.viterbi;

import nl.utwente.db.ZehminCRF.corpus.Sentence;

/**
 * @author Zhemin Zhu
 * Created on Sep 14, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public interface StepGain {
	public double gain0(Sentence sentence, String tag);
	public double gain(Sentence s, int i, String preTag, String curTag);
}
