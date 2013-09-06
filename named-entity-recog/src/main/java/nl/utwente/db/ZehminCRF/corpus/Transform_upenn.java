package nl.utwente.db.ZehminCRF.corpus;

import nl.utwente.db.ZehminCRF.utils.FeatureFunctions;
import nl.utwente.db.ZehminCRF.utils.Global;

/**
 * @author Zhemin Zhu
 * Created on Apr 16, 2013
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class Transform_upenn {
	
	public static void main(String[] strs){
		String desktop = "D:/Profiles/ZhuZ/Desktop/WSJ_pos/";
		transform(desktop + "WSJ_pos.dev");
		transform(desktop + "WSJ_pos.test");
		transform(desktop + "WSJ_pos.train");
	}

	public static void transform(String sourceFile){
		String corpus = Global.readFile(sourceFile);
		String targetFile = sourceFile+".tran";
		StringBuilder output = new StringBuilder();
		String[] lines = corpus.split("\n");
		for(String line : lines){
			if(line.equals("<utt>")){
				output.append("\n");
				continue;
			}
			String[] items = line.split("\t");
			String word = items[0];
			String pos = items[1];
			output.append(word + "\t" + FeatureFunctions.ff(word) + "\t" + pos + "\n");
		}
		Global.writeOutput(output.toString(), targetFile);
		System.out.println("Transform Finished:\t" + sourceFile + " -> " + targetFile);
	}
	
	
}
