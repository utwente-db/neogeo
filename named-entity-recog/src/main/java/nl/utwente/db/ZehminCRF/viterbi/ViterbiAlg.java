package nl.utwente.db.ZehminCRF.viterbi;

import java.util.Vector;

import nl.utwente.db.ZehminCRF.corpus.Sentence;
import nl.utwente.db.ZehminCRF.sp.CRModel_sp1;

/**
 * @author Zhemin Zhu
 * Created on Aug 22, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class ViterbiAlg {
	Vector<String> m_tagSpace;
	StepGain m_stepGain;
	CRModel_sp1 m_model;
	
	public ViterbiAlg(
			StepGain stepGain, 
			CRModel_sp1 model, 
			boolean bFullTagSpace){
		m_stepGain = stepGain;
		m_model = model;
	}
	
	
	
	
	
	
	public Vector<String> decode(Sentence s){
		MatrixTGP matrix = null;
		matrix = new MatrixTGP(s, m_model);
		//gain [0]
		Vector<TagGainPre> vecTGP = matrix.getFirstColumn();
		for(TagGainPre tgp : vecTGP){
			tgp.setGain(m_stepGain.gain0(s, tgp.getTag()));
		}
		
		//gain [1:Last]
		for(int i = 1; i < matrix.getNumColum(); ++ i){
			Vector<TagGainPre> vecPreTGP = matrix.getColumn(i - 1);
			Vector<TagGainPre> vecCurTGP = matrix.getColumn(i);
			boolean bNoPromisingTag = true;
			for(TagGainPre curTGP : vecCurTGP){
				double maxGain = Double.NEGATIVE_INFINITY;
				int maxPre = -1;
				for(int j = 0; j < vecPreTGP.size(); ++ j){
					TagGainPre preTGP = vecPreTGP.get(j);
					double gain = Double.NEGATIVE_INFINITY;
					gain = m_stepGain.gain(s, i, preTGP.getTag(), curTGP.getTag()) + preTGP.getGain();
					
					
					//note:kdkd
						/*if(gain > 0)
							gain = 0;*/
					
					if(gain > maxGain){
						maxGain = gain;
						maxPre = j;
					}
				}
				if(maxGain > Double.NEGATIVE_INFINITY){
					curTGP.setGain(maxGain);
					curTGP.setPre(maxPre);
					bNoPromisingTag = false;
				}
			}
			if(bNoPromisingTag)
				matrix.noPromisingTag(s, i);
		}
		
		//matrix.printMatrix();
		return matrix.findMaxPath();
	}
	
}
	
