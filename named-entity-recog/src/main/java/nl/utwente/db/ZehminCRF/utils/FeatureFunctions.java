package nl.utwente.db.ZehminCRF.utils;

/**
 * @author Zhemin Zhu
 * Created on Jul 9, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 * 
 * X is the observed sentence, Y are the taggs.
 */
public class FeatureFunctions {
	
	public static String ff(String w){
		return ff_ending(w) + "\t" + ff_digitupper(w) + "\t" + ff_hyphen(w);
	}
	
    /* word feature
	 * ending
	 */
	public static String ff_ending(String word) {
		int length = word.length();
		//if(word.equals(Global.g_startWord) || word.equals(Global.g_endWord))
			//return word;
		
		if(length >= 4){
			String end = word.substring(length - 4, length);
			if(end.equals("tion"))
				return "t";
		}
		
		if(length >= 3){
			String end = word.substring(length - 3, length);
			if(end.equals("ing") || end.equals("ogy") 
					|| end.equals("ion") || end.equals("ity") || end.equals("ies"))
				return "t";
		}
		
		if(length >= 2){
			String end = word.substring(length - 2, length);
			if(end.equals("ed") || end.equals("ly"))
				return "t";
		}
		
		if(length >= 1){
			String end = word.substring(length - 1, length);
			if(end.equals("s"))
				return "t";
		}
		
		return "f";

	}
	
	
	/* word feature
	 * Beginning with a number or upper case letter
	 */
	public static String ff_digitupper(String word) {
		//if(word.equals(Global.g_startWord) || word.equals(Global.g_endWord))
			//return word;
		char ch = word.charAt(0);
		if((ch <= '9' && ch >= '0') || (ch <= 'Z' && ch >= 'A'))
			return "t";
		else 
			return "f";
	}
	
	
	/* word feature
	 * including hyphen
	 */
	public static String ff_hyphen(String word) {
		//if(word.equals(Global.g_startWord) || word.equals(Global.g_endWord))
			//return word;
		if(word.indexOf('-')!= -1)
			return "t";
		else
			return "f";
	}
	
}
