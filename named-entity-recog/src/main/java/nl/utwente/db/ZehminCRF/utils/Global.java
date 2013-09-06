package nl.utwente.db.ZehminCRF.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import nl.utwente.db.ZehminCRF.corpus.Corpus;
import nl.utwente.db.ZehminCRF.corpus.Sentence;

/**
 * @author Zhemin Zhu
 * Created on Aug 22, 2012
 *
 * CTIT Database Group
 * Universiteit Twente
 */
public class Global {
	//linux: java -Xmx6g -jar *** > log_file &
	
	public static  boolean g_bWindows = true;
	public static  boolean g_bWithStartEndSymbols = true;
	public static  boolean g_bPSSOODecompose = true;
	public static  boolean g_bCPTF = true;   //the method to construct CPTF. false: use parameter. true: use counting
	
	

	
	public static final String g_desktop_windows = "D:/Profiles/ZhuZ/Desktop/";
	public static final String g_desktop_linux = "/local/home/zhemin/";
	
	public static String g_BrownCorpusFolder;
	public static String g_BrwonCorpusFile;
	public static String g_BrwonModelFile;
	
	
	
	
	public static  String g_CRModelFolder;
	public static  String g_LBPExp1CorpusFolder;
	public static  String g_MixedOrderCorpusFolder;

	
	public static  String g_upennCorpusFolder = "D:/Profiles/ZhuZ/Desktop/upenn";
	public static  String g_PMIModelFolder = "D:/Profiles/ZhuZ/Desktop/pmi/";
	public static  String g_CountingModelFolder = "D:/Profiles/ZhuZ/Desktop/counting/";
	public static  String g_MEMMCountingModelFolder = "D:/Profiles/ZhuZ/Desktop/MEMMs/";
	public static  String g_PMICRFModelFolder = "D:/Profiles/ZhuZ/Desktop/PMICRF/";
	public static  String g_PMICRFModel2Folder = "D:/Profiles/ZhuZ/Desktop/PMICRF2/";
	public static  String g_NERFolder = "D:/Profiles/ZhuZ/Desktop/Name_entity/";
	
	
	static{
		if(g_bWindows){
			g_BrownCorpusFolder = g_desktop_windows + "brown/brown_orig/";
			g_BrwonCorpusFile = g_desktop_windows + "brown/brown_trans/brown_corpus";
			g_BrwonModelFile = g_desktop_windows + "brown/brown_trans/brown_model";
			
			g_CRModelFolder = g_desktop_windows + "CR/";
			g_LBPExp1CorpusFolder = g_desktop_windows + "lbp_1/";
			g_MixedOrderCorpusFolder = g_desktop_windows + "mixed_order/";
			
		}else{
			g_BrownCorpusFolder = g_desktop_linux + "brown";
			g_BrwonCorpusFile = g_desktop_linux + "brown_corpus";
			g_BrwonModelFile = g_desktop_linux + "brown_model";
			g_CRModelFolder = g_desktop_linux + "CR/";
			g_LBPExp1CorpusFolder = g_desktop_linux + "lbp_1/";
			g_MixedOrderCorpusFolder = g_desktop_linux + "mixed_order/";
		}
	}
	
	
	
	
	public static final String g_startTag = "starttagkyuj";
	public static final String g_endTag = "endtagkyuj";
	public static final String g_startWord = "startwordkyuj";
	public static final String g_endWord = "endwordkyuj";
	public static Random random_generator = new Random(); 
	
	
	
	public static void saveParameters(HashMap<String, PG> parameters, String path) {
		
		try{
			  FileWriter fstream = new FileWriter(path);
			  BufferedWriter out = new BufferedWriter(fstream);
			  int num = 0;
			  Iterator it = parameters.entrySet().iterator();
			  while(it.hasNext()){
				  Map.Entry pairs = (Map.Entry)it.next();
				  String key = (String)pairs.getKey();
				  PG pg = (PG)pairs.getValue();
				  out.append(key + " " + pg.getParameterValue() + " " + pg.getGradient() + "\n");
				  ++ num;
				  if(num % 100000 == 0)
					out.flush();
			  }
			  out.flush();
			  out.close();
		}catch (Exception e){//Catch exception if any
			  System.err.println("Error: " + e.getMessage());
		}
		
	}
	
	
	public static void addCNTInt(
			String key, 
			Integer cntAdded, 
			HashMap<String, Integer> map)
	{
		if(map.containsKey(key)){
			map.put(key, map.get(key) + cntAdded);
		}else{
			map.put(key, cntAdded);
		}
	}
	
	
	public static void addCNTDouble(
			String key, 
			Double cntAdded, 
			HashMap<String, Double> map)
	{
		if(map.containsKey(key)){
			map.put(key, map.get(key) + cntAdded);
		}else{
			map.put(key, cntAdded);
		}
	}
	
	
	public static Vector<String> addStartEndWords(Vector<String> words){
		words.add(0, g_startWord);
		words.add(g_endWord);
		return words;
	}
	
	public static Vector<String> addStartEndFetures(Vector<String> fes){
		fes.add(0, FeatureFunctions.ff(g_startWord).replaceAll("\t", "_"));
		fes.add(FeatureFunctions.ff(g_endWord).replaceAll("\t", "_"));
		return fes;
	}
	
	public static Vector<String> addStartEndTags(Vector<String> tags){
		tags.add(0, g_startTag);
		tags.add(g_endTag);
		return tags;
	}
	
	public static int getRandomInt(int n){
		return random_generator.nextInt(n);
	}
	
	public static int getRandomInt(){
		return random_generator.nextInt();
	}
	
	public static double log2e(double d){
		return Math.pow(Math.E, d);
	}
	
	public static String printVectorString(Vector<String> vecStr){
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < vecStr.size(); ++ i){
			sb.append(i + ". " + vecStr.get(i) + "\n");
		}
		return sb.toString();
	}
	
	/*
	 * reads all the content in a file 
	 */
	public static String readFile(String strFilePath){
		try{
				File annFile = null;
				if ( true ) {
					ClassLoader classLoader = new Global().getClass().getClassLoader();
					URL url = classLoader.getResource(strFilePath);
					if ( url == null )
						throw new RuntimeException("Global:readFile: unable to locate resource: "+strFilePath);
	        		annFile = new File(url.getFile());
	        		System.out.println("FOUND: "+url);
				} else 
					annFile = new File(strFilePath);
				int fileSize = (int) annFile.length();
				char[] buff = new char[fileSize]; 
				FileInputStream fin = new FileInputStream(annFile);
				BufferedReader br = new BufferedReader(new InputStreamReader(fin));
				br.read(buff, 0, fileSize);
				fin.close();
				return new String(buff);
		}catch(Exception exp){
			exp.printStackTrace();
			return null;
		}
		
	}
	
	/*
	 * reads all the files in a folder, and returns the content as a string. 
	 */
	public static String readFolder(String strFolderPath){
		try{
			StringBuilder sbResult = new StringBuilder();
			File file = new File(strFolderPath);
			String[] annFiles = file.list();
			for(String strAnnFile : annFiles){
				String strFilePath = strFolderPath + "/" + strAnnFile;
				File annFile = new File(strFilePath);
				int fileSize = (int) annFile.length();
				char[] buff = new char[fileSize]; 
				FileInputStream fin = new FileInputStream(strFilePath);
				BufferedReader br = new BufferedReader(new InputStreamReader(fin));
				br.read(buff, 0, fileSize);
				sbResult.append(buff);
				fin.close();
			}
			return sbResult.toString();
		}catch(Exception exp){
			exp.printStackTrace();
			return null;
		}
		
	}
	
	/*
	 * write a string to a file
	 */
	public static void writeOutput(String content, String strPath){
		try{
			File f = new File(strPath);
			BufferedWriter output = new BufferedWriter(new FileWriter(f));   
			output.write(content);
			output.flush();
			output.close();
		}catch(Exception exp){
			exp.printStackTrace();
		}
	}

	public static String[]  getWordTag(String wordTag){
		int index = -1;
		for(int i = wordTag.length() - 1; i > 0 ; -- i){
			char ch = wordTag.charAt(i);
			if(ch == '/'){
				index = i;
				break;
			}
		}
		String[] result = new String[2];
		result[0] = wordTag.substring(0, index);
		result[1] = wordTag.substring(index + 1, wordTag.length());
		return result;
	}
	
	public static boolean isTag(String str){
		return (str.indexOf("_tag") != -1);
	}
	
	/**
	 * @author Zhemin Zhu
	 * Created on Jul 13, 2012
	 *
	 * CTIT Database Group
	 * Universiteit Twente
	 * 
	 * crf_learn template_file train_file model_file
	 * crf_test -m model_file test_files > result
	 * 
	 * Compile CRF++: see the INSTALL file in the source folder
	 * 
	 * linux: 
	 * 
	 * PATH=$PATH:/usr/local/bin
	 * export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
	 * crf_learn template train.data model > train_info &
	 * crf_test -m model test.data > result
	 * 
	 * 
	 * 
	 * 95.41%
	 * 
	 * 4571806s
	 * 
	 * Precision: 95.41%
	 * OOV Precision: 71.70%
	 * OOV rate: 1.873456720204789%
	 * No OOV: 96.112%
	 * Self-testing:
	 */
	public static double crfpp_precision(int numColumn, String resultFile, Corpus training_corpus){
		int correct = 0;
		int total = 0;
		int totalOOV = 0;
		int corrOOV = 0;
		HashMap<String, Boolean> mp = training_corpus.getMapWords();
		try{
            FileInputStream f = new FileInputStream(resultFile);  
            byte[] buf = new byte[f.available()];      
            f.read(buf, 0, f.available());   
            f.close();
            String result = new String(buf);
            String[] lines = result.split("\n");
            for(String line : lines){
            	boolean bOOV = false;
            	line = line.trim();
            	if(line.length() > 0){
            		++ total;
            		String[] strs = line.split("\t");
            		if(! mp.containsKey(strs[0])) bOOV = true;
            		if(bOOV) ++ totalOOV;
            		if(strs[numColumn - 2].equals(strs[numColumn - 1])){
            			++ correct;
            			if(bOOV) ++ corrOOV;
            		}
            	}
            }
		}catch(IOException ex){
			ex.printStackTrace();
		}
		System.out.println("crfpp_precision_total: " + correct*1.0/total);
		System.out.println("crfpp_precision_nonOOV: " + (correct-corrOOV) * 1.0 / (total - totalOOV));
		System.out.println("crfpp_precision_OOV: " + corrOOV*1.0/totalOOV);
		return correct * 1.0 / total;
	}
	
	public static double mallet_precision(String resultFile, Corpus training_corpus, Corpus decoding_corpus){
		int correct = 0;
		int total = 0;
		int totalOOV = 0;
		int corrOOV = 0;
		HashMap<String, Boolean> mp = training_corpus.getMapWords();
		Vector<String> corrTags = new Vector<String>();
		Vector<String> corrWords = new Vector<String>();
		for(int i = 0; i < decoding_corpus.getNumSentences(); ++ i){
			Sentence s = decoding_corpus.getSentence(i);
			for(int j = 0; j < s.size(); ++ j){
				corrTags.add(s.getTag(j));
				corrWords.add(s.getWord(j));
			}
		}
		
		try{
            FileInputStream f = new FileInputStream(resultFile);  
            byte[] buf = new byte[f.available()];      
            f.read(buf, 0, f.available());   
            f.close();
            String result = new String(buf);
            String[] lines = result.split("\n");
            int j = 0;
            for(int i = 0; i < lines.length; ++ i){
            	String line = lines[i];
            	line = line.trim();
            	boolean bOOV = false;
            	if(line.length() > 0){
            		++ total;
            		if(! mp.containsKey(corrWords.get(j))) bOOV = true;
            		if(bOOV){
            			++ totalOOV;
            		}
            		
            		if(line.equals(corrTags.get(j))){
            			++ correct;
            			if(bOOV) ++ corrOOV;
            		}
            		
            		//System.out.println(corrWords.get(j) + "\t" 
                    		//+ line + "=>" + corrTags.get(j) + "\t" + bOOV);
            		
            		++ j;
            	}
            }
		}catch(IOException ex){
			ex.printStackTrace();
		}
		//System.out.println("#OOVs: " + totalOOV);
		//System.out.println("#corrOOVs: " + corrOOV);
		//System.out.println("#Total: " + total);
		//System.out.println("#corrTotal: " + correct);
		
		System.out.println("mallet_precision_total: " + correct*1.0/total);
		System.out.println("mallet_precision_nonOOV: " + (correct-corrOOV) * 1.0 / (total - totalOOV));
		System.out.println("mallet_precision_OOV: " + corrOOV*1.0/totalOOV);
		System.out.println("==================\n\n");
		return correct * 1.0 / total;
	}
	
}
