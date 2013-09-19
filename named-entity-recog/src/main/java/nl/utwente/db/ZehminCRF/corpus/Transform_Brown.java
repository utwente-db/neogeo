package nl.utwente.db.ZehminCRF.corpus;

import java.util.HashMap;
import java.util.Vector;

import nl.utwente.db.ZehminCRF.utils.FeatureFunctions;
import nl.utwente.db.ZehminCRF.utils.Global;

/**
 * @author Zhemin Zhu
 * Created on Oct 12, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class Transform_Brown {
	
	public static void main(String[] strs){
		transform(Global.g_BrownCorpusFolder, Global.g_BrwonCorpusFile);
		Corpus corpus = new Corpus(Global.g_BrwonCorpusFile,false);
		corpus.printStatistics();
		System.out.println("Transform Finished!");
	}
	
	public static void transform(String tagAnnFolder, String targetFile){
		String corpus = Global.readFolder(tagAnnFolder);
		String[] lines = corpus.split("\n");
		HashMap<String, Boolean> mp_tag = new HashMap<String, Boolean>();
		Vector<Sentence> sentences = new Vector<Sentence>();
		StringBuilder sb = new StringBuilder();
		for(String line : lines){
			line = line.trim();
			if(line.length() > 0 && line.indexOf("-hl") == -1 && line.indexOf("-tl") == -1){
				Vector<String> words = new Vector<String>();
				Vector<String> tags = new Vector<String>();
				String[] tokenTags = line.split(" ");
				for(String tokenTag : tokenTags){
					tokenTag = tokenTag.trim();
					if(tokenTag.length() == 0)
						continue;
					String[] strs = Global.getWordTag(tokenTag);
					if(strs[0].length() > 0 && strs[1].length() > 0){
						words.add(strs[0]);
						tags.add(strs[1]+"_tag");
					}
				}
				if(isCompleteSentece(words)){
					/*if(Global.g_bWithStartEndSymbols){
						Global.addStartEndWords(words);
						Global.addStartEndTags(tags);
					}*/
					for(int i = 0; i < words.size(); ++ i){
						String word = words.get(i);
						String tag = tags.get(i);
						sb.append(word + "\t" + FeatureFunctions.ff(word) + "\t" + tag + "\n");
					}
					sb.append("\n");
				}
			}
		}
		
		Global.writeOutput(sb.toString(), targetFile);
	}
	
	static boolean isCompleteSentece(Vector<String> words){
		if(!words.lastElement().equals("."))
			return false;
		char ch = words.firstElement().charAt(0);
		return (ch <= '9' && ch >= '0') || (ch <= 'Z' && ch >= 'A');
	}
	
}
