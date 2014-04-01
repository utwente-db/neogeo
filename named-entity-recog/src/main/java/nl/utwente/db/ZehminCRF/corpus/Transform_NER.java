package nl.utwente.db.ZehminCRF.corpus;

import java.util.Vector;

import nl.utwente.db.ZehminCRF.utils.FeatureFunctions;
import nl.utwente.db.ZehminCRF.utils.Global;

/**
 * @author Zhemin Zhu
 * Created on Oct 18, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class Transform_NER {
	public static void main(String[] strs){
		//transform(Global.g_desktop_windows + "NER/NER_orig/ned.train", Global.g_desktop_windows + "NER/NER_trans/ned.train" );
		//transform(Global.g_desktop_windows + "NER/NER_orig/ned.testa", Global.g_desktop_windows + "NER/NER_trans/ned.testa" );
		//transform(Global.g_desktop_windows + "NER/NER_orig/ned.testb", Global.g_desktop_windows + "NER/NER_trans/ned.testb" );
		Global.g_bWithStartEndSymbols = false;
		Corpus corpus = new Corpus(null,Global.g_desktop_windows + "NER/NER_trans/ned.train",false );
		corpus.transform2MALLET(Global.g_desktop_windows + "NER/NER_mallet_all/ned.train_mallet" );
		corpus.transform2MALLET_more1(Global.g_desktop_windows + "NER/NER_mallet_more1/ned.train_mallet" );
		corpus.printStatistics();
		
		corpus = new Corpus(null,Global.g_desktop_windows + "NER/NER_trans/ned.testa",false );
		corpus.transform2MALLET(Global.g_desktop_windows + "NER/NER_mallet_all/ned.testa_mallet" );
		corpus.transform2MALLET_more1(Global.g_desktop_windows + "NER/NER_mallet_more1/ned.testa_mallet" );
		corpus.printStatistics();
		
		corpus = new Corpus(null,Global.g_desktop_windows + "NER/NER_trans/ned.testb",false );
		corpus.transform2MALLET(Global.g_desktop_windows + "NER/NER_mallet_all/ned.testb_mallet" );
		corpus.transform2MALLET_more1(Global.g_desktop_windows + "NER/NER_mallet_more1/ned.testb_mallet" );
		corpus.printStatistics();
		
		System.out.println("Transform Finished!");
	}
	
	public static void transform(String originalFile, String targetFile){
		String filebuffer = Global.readFile(originalFile);
		String[] strSentences = filebuffer.split("\n\n");
		StringBuilder sb = new StringBuilder();
		for(String strSentence : strSentences){
			String[] lines = strSentence.split("\n");
			Vector<String> words = new Vector<String>();
			Vector<String> tags = new Vector<String>();
			for(String line : lines){
				String[] strs = line.split(" ");
				words.add(strs[0]);
				tags.add(strs[2]);
			}
			if(!bFilter(words)){
				for(int i = 0; i < words.size(); ++ i){
					String word = words.get(i);
					String tag = tags.get(i);
					sb.append(word + "\t" + FeatureFunctions.ff(word) + "\t" + tag + "\n");
				}
				sb.append("\n");
			}
		}
		Global.writeOutput(sb.toString(), targetFile);
	}
	
	public static boolean bFilter(Vector<String> words){
		return !(words.size() > 1);
	}
	
}
