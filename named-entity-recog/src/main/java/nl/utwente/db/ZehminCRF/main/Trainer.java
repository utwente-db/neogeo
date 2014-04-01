package nl.utwente.db.ZehminCRF.main;

import nl.utwente.db.ZehminCRF.corpus.Corpus;
import nl.utwente.db.ZehminCRF.sp.OptimizerPMI;
import nl.utwente.db.ZehminCRF.utils.Global;
import cc.mallet.optimize.LimitedMemoryBFGS;
import cc.mallet.optimize.Optimizer;



/*Note: 
 * the following points can affect the results significantly:
 * 1. treat start and end symbol as normal words with feature (ffff)
 * 2. CR(y;y'|X) = P(y,y'|X) / P(y, *|X) / P(*, y' | X)
 * 3. when constructing CPTF, each observe can only be counted once.
 */




/**
 * @author Zhemin Zhu
 * Created on Aug 22, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class Trainer {
	
	 public static void main(String[] strs){
		//this line is for fixing the JDK bug.
		//this bug happens in the method Collections.sort().
		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		 
		 //Brown
		String modelFile = Global.g_BrwonModelFile;
		Corpus brown_corpus = new Corpus(null,Global.g_BrwonCorpusFile,false);
		Corpus training_corpus = brown_corpus.genSubCorpus(0, 100);
		//NER 
		/*String modelFile = Global.g_desktop_windows + "NER/NER_trans/ner_model";
		Corpus ner_corpus = new Corpus(Global.g_desktop_windows + "NER/NER_trans/ned.train");
		Corpus training_corpus = ner_corpus; 
		*/
		
		//CRModel_sp1 model = new CRModel_sp1(training_corpus, modelFile, true);
		//model.train();
		
		System.out.println("Reading Training Corpus... ");
		System.out.println("Training Corpus: ");
		training_corpus.printStatistics();
		OptimizerPMI optimizable = new OptimizerPMI(training_corpus, modelFile);
        Optimizer optimizer = new LimitedMemoryBFGS(optimizable);
        boolean converged = false;
        try {
            converged = optimizer.optimize();
            optimizable.m_timer.end();
            optimizable.m_timer.printTime();
            System.out.println("Converged: " + converged);
            optimizable.saveParameters();
        } catch (IllegalArgumentException e) {
        	e.printStackTrace();
            // This exception may be thrown if L-BFGS
            //  cannot step in the current direction.
            // This condition does not necessarily mean that
            //  the optimizer has failed, but it doesn't want
            //  to claim to have succeeded...

        }
        System.out.println("Finished Successfully!");
    }
	
}
