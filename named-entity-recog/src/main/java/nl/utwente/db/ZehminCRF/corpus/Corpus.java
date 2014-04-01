package nl.utwente.db.ZehminCRF.corpus;

import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import nl.utwente.db.ZehminCRF.sp.CPT;
import nl.utwente.db.ZehminCRF.utils.Global;
import nl.utwente.db.ZehminCRF.utils.StringInteger;
import nl.utwente.db.neogeo.utils.FileUtils;

/**
 * @author Zhemin Zhu Created on Jul 20, 2012
 *
 * CTIT Database Group Universiteit Twente
 */
public class Corpus
{
    private Vector<Sentence> m_sentences;
    private HashMap<String, String> m_mapOF;

    private static final Hashtable<String,String[]> scache = new Hashtable<String,String[]>();
    
	public Corpus(String dirPath, String filePath, boolean use_cache) {
		String[] sentences = scache.get(filePath);
		
		if (use_cache && sentences != null) {
			System.out.println("#!Cache-hit: " + filePath);
		} else {
			System.out.println("#!Cache-miss["+use_cache+"]: " + filePath);

			if  (dirPath == null )
				dirPath = "";
            String buf = FileUtils.getFileAsString(new File(dirPath + filePath));
			sentences = buf.split("\n\n");
			if ( use_cache )
				scache.put(filePath, sentences);
		}
		m_sentences = new Vector<Sentence>();
		for (String sentence : sentences) {
			m_sentences.add(new Sentence(sentence));
		}
		initMapOF();
	}

    public Corpus(Vector<Sentence> sentences)
    {
        m_sentences = sentences;
        initMapOF();
    }

    public Corpus merge(Corpus newCorpus)
    {
        m_sentences.addAll(newCorpus.getAllSentences());
        return new Corpus(m_sentences);
    }

    public Vector<Sentence> getAllSentences()
    {
        return m_sentences;
    }

    private void initMapOF()
    {
        m_mapOF = new HashMap<String, String>();
        for (Sentence s : m_sentences)
        {
            for (int i = 0; i < s.size(); ++i)
            {
                String o = s.getWord(i);
                String f = s.getFeature(i);
                if (!m_mapOF.containsKey(o))
                {
                    m_mapOF.put(o, f);
                }
            }
        }
    }
    Vector<StringInteger> m_vecNumSSFF;

    public Vector<StringInteger> getNumSSFF()
    {
        if (m_vecNumSSFF != null)
        {
            return m_vecNumSSFF;
        }
        HashMap<String, Integer> mapSSFF = new HashMap<String, Integer>();
        for (int i = 0; i < getNumSentences(); ++i)
        {
            Sentence sentence = m_sentences.get(i);
            for (int j = 0; j < sentence.size() - 1; ++j)
            {
                String ssff = sentence.getTag(j) + " " + sentence.getTag(j + 1) + " "
                        + sentence.getFeature(j) + " " + sentence.getFeature(j + 1);
                if (!mapSSFF.containsKey(ssff))
                {
                    mapSSFF.put(ssff, 1);
                }
                else
                {
                    mapSSFF.put(ssff, mapSSFF.get(ssff) + 1);
                }
            }
        }
        m_vecNumSSFF = new Vector<StringInteger>();
        Set<String> keys = mapSSFF.keySet();
        for (String key : keys)
        {
            m_vecNumSSFF.add(new StringInteger(key, mapSSFF.get(key)));
        }
        return m_vecNumSSFF;
    }
    Vector<StringInteger> m_vecNumSF;

    public Vector<StringInteger> getNumSF()
    {
        if (m_vecNumSF != null)
        {
            return m_vecNumSF;
        }
        HashMap<String, Integer> mapSF = new HashMap<String, Integer>();
        for (int i = 0; i < getNumSentences(); ++i)
        {
            Sentence sentence = m_sentences.get(i);
            for (int j = 0; j < sentence.size() - 1; ++j)
            {
                String sf = sentence.getTag(j) + " " + sentence.getFeature(j);
                if (!mapSF.containsKey(sf))
                {
                    mapSF.put(sf, 1);
                }
                else
                {
                    mapSF.put(sf, mapSF.get(sf) + 1);
                }
            }
        }
        m_vecNumSF = new Vector<StringInteger>();
        Set<String> keys = mapSF.keySet();
        for (String key : keys)
        {
            m_vecNumSF.add(new StringInteger(key, mapSF.get(key)));
        }
        return m_vecNumSF;
    }
    Vector<StringInteger> m_vecNumFF;

    public Vector<StringInteger> getNumFF()
    {
        if (m_vecNumFF != null)
        {
            return m_vecNumFF;
        }
        HashMap<String, Integer> mapFF = new HashMap<String, Integer>();
        for (int i = 0; i < getNumSentences(); ++i)
        {
            Sentence sentence = m_sentences.get(i);
            for (int j = 0; j < sentence.size() - 1; ++j)
            {
                String ff = sentence.getFeature(j) + " " + sentence.getFeature(j + 1);
                if (!mapFF.containsKey(ff))
                {
                    mapFF.put(ff, 1);
                }
                else
                {
                    mapFF.put(ff, mapFF.get(ff) + 1);
                }
            }
        }
        m_vecNumFF = new Vector<StringInteger>();
        Set<String> keys = mapFF.keySet();
        for (String key : keys)
        {
            m_vecNumFF.add(new StringInteger(key, mapFF.get(key)));
        }
        return m_vecNumFF;
    }
    Vector<String> m_vecTagSpace;

    public Vector<String> getTagSpace()
    {
        if (m_vecTagSpace != null)
        {
            return m_vecTagSpace;
        }
        HashMap<String, Boolean> mapS = new HashMap<String, Boolean>();
        m_vecTagSpace = new Vector<String>();
        for (int i = 0; i < getNumSentences(); ++i)
        {
            Sentence sentence = m_sentences.get(i);
            for (int j = 0; j < sentence.size(); ++j)
            {
                String s = sentence.getTag(j);
                if (!mapS.containsKey(s))
                {
                    m_vecTagSpace.add(s);
                    mapS.put(s, true);
                }
            }
        }
        return m_vecTagSpace;
    }
    HashMap<String, Double> m_mapEmpProbS;

    public HashMap<String, Double> constructEmpProbS()
    {
        if (m_mapEmpProbS != null)
        {
            return m_mapEmpProbS;
        }
        HashMap<String, Double> mapS = new HashMap<String, Double>();
        double sumS = 0;
        for (int i = 0; i < getNumSentences(); ++i)
        {
            Sentence sentence = m_sentences.get(i);
            for (int j = 0; j < sentence.size(); ++j)
            {
                String s = sentence.getTag(j);
                Global.addCNTDouble(s, 1.0, mapS);
                ++sumS;
            }
        }
        Set<String> keys = mapS.keySet();
        m_mapEmpProbS = new HashMap<String, Double>();
        for (String s : keys)
        {
            double ps = Math.log(mapS.get(s) / sumS);
            m_mapEmpProbS.put(s, ps);
        }
        return m_mapEmpProbS;
    }
    HashMap<String, Double> m_mapEmpCRSS;

    public HashMap<String, Double> constructEmpCRSS()
    {
        if (m_mapEmpCRSS != null)
        {
            return m_mapEmpCRSS;
        }
        HashMap<String, Double> mapCRSS = new HashMap<String, Double>();
        HashMap<String, Integer> mapSS = new HashMap<String, Integer>();
        int sumSS = 0;
        HashMap<String, Integer> mapS = new HashMap<String, Integer>();
        int sumS = 0;
        for (int i = 0; i < getNumSentences(); ++i)
        {
            Sentence sentence = m_sentences.get(i);
            for (int j = 0; j < sentence.size() - 1; ++j)
            { //CR
                String s1 = sentence.getTag(j);
                String s2 = sentence.getTag(j + 1);
                Global.addCNTInt(s1 + " " + s2, 1, mapSS);
                ++sumSS;
            }

            for (int j = 0; j < sentence.size(); ++j)
            {
                String s = sentence.getTag(j);
                Global.addCNTInt(s, 1, mapS);
                ++sumS;
            }
        }

        Set<String> keys = mapSS.keySet();
        for (String ss : keys)
        {
            String[] strs = ss.split(" ");
            double pss = 1.0 * mapSS.get(ss) / sumSS;
            double ps1 = 1.0 * mapS.get(strs[0]) / sumS;
            double ps2 = 1.0 * mapS.get(strs[1]) / sumS;
            double cr = Math.log(pss / ps1 / ps2);
            mapCRSS.put(ss, cr);
        }
        m_mapEmpCRSS = mapCRSS;
        return m_mapEmpCRSS;
    }
    Vector<CPT> m_vecCPTs;

    public Vector<CPT> getEmpCPTs()
    {
        if (m_vecCPTs != null)
        {
            return m_vecCPTs;
        }

        m_vecCPTs = new Vector<CPT>();
        HashMap<String, CPT> cpto = constructCPTO();
        HashMap<String, CPT> cptf = constructCPTF();
        HashMap<String, CPT> cptoo = constructCPTOO();
        HashMap<String, CPT> cptff = constructCPTFF();
        HashMap<String, CPT> cptff1 = constructCPTFF1();
        HashMap<String, CPT> cptff2 = constructCPTFF2();

        Set<String> keys = cpto.keySet();
        for (String key : keys)
        {
            m_vecCPTs.add(cpto.get(key));
        }

        keys = cptf.keySet();
        for (String key : keys)
        {
            m_vecCPTs.add(cptf.get(key));
        }

        keys = cptoo.keySet();
        for (String key : keys)
        {
            m_vecCPTs.add(cptoo.get(key));
        }

        keys = cptff.keySet();
        for (String key : keys)
        {
            m_vecCPTs.add(cptff.get(key));
        }

        keys = cptff1.keySet();
        for (String key : keys)
        {
            m_vecCPTs.add(cptff1.get(key));
        }

        keys = cptff2.keySet();
        for (String key : keys)
        {
            m_vecCPTs.add(cptff2.get(key));
        }

        return m_vecCPTs;
    }
    HashMap<String, CPT> m_mapCPTO = null;

    public HashMap<String, CPT> constructCPTO()
    {
        if (m_mapCPTO != null)
        {
            return m_mapCPTO;
        }
        HashMap<String, CPT> mapCPTO = new HashMap<String, CPT>();
        for (Sentence s : m_sentences)
        {
            for (int i = 0; i < s.size(); ++i)
            {
                String o = s.getWord(i);
                String t = s.getTag(i);
                if (mapCPTO.containsKey(o))
                {
                    mapCPTO.get(o).addTag(t, 1.0);
                }
                else
                {
                    CPT cpto = new CPT(o);
                    cpto.addTag(t, 1.0);
                    mapCPTO.put(o, cpto);
                }
            }
        }
        m_mapCPTO = mapCPTO;
        return m_mapCPTO;
    }
    HashMap<String, CPT> m_mapCPTOO = null;

    public HashMap<String, CPT> constructCPTOO()
    {
        if (m_mapCPTOO != null)
        {
            return m_mapCPTOO;
        }
        HashMap<String, CPT> mapCPTOO = new HashMap<String, CPT>();
        for (Sentence s : m_sentences)
        {
            for (int i = 0; i < s.size() - 1; ++i)
            {
                String oo = s.getWord(i) + " " + s.getWord(i + 1);
                String tt = s.getTag(i) + " " + s.getTag(i + 1);
                if (mapCPTOO.containsKey(oo))
                {
                    mapCPTOO.get(oo).addTag(tt, 1.0);
                }
                else
                {
                    CPT cptoo = new CPT(oo);
                    cptoo.addTag(tt, 1.0);
                    mapCPTOO.put(oo, cptoo);
                }
            }
        }
        m_mapCPTOO = mapCPTOO;
        return m_mapCPTOO;
    }
    HashMap<String, CPT> m_mapCPTF = null;

    public HashMap<String, CPT> constructCPTF()
    {
        if (m_mapCPTF != null)
        {
            return m_mapCPTF;
        }
        HashMap<String, CPT> mapCPTO = constructCPTO();
        HashMap<String, CPT> mapCPTF = new HashMap<String, CPT>();
        Set<String> keys = mapCPTO.keySet();
        for (String o : keys)
        {
            CPT cpto = mapCPTO.get(o);
            String f = m_mapOF.get(o);
            CPT cptf = mapCPTF.get(f);
            if (cptf == null)
            {
                cptf = new CPT(f);
                mapCPTF.put(f, cptf);
            }
            Vector<String> tagSpace = cpto.getTagSpace();
            for (String tag : tagSpace)
            {
                cptf.addTag(tag, cpto.getProbNonLog(tag));
            }
        }
        m_mapCPTF = mapCPTF;
        return m_mapCPTF;
    }
    HashMap<String, CPT> m_mapCPTFF = null;

    public HashMap<String, CPT> constructCPTFF()
    {
        if (m_mapCPTFF != null)
        {
            return m_mapCPTFF;
        }
        HashMap<String, CPT> mapCPTOO = constructCPTOO();
        HashMap<String, CPT> mapCPTFF = new HashMap<String, CPT>();
        Set<String> keys = mapCPTOO.keySet();
        for (String oo : keys)
        {
            CPT cptoo = mapCPTOO.get(oo);
            String ff = m_mapOF.get(oo.split(" ")[0]) + " " + m_mapOF.get(oo.split(" ")[1]);
            CPT cptff = mapCPTFF.get(ff);
            if (cptff == null)
            {
                cptff = new CPT(ff);
                mapCPTFF.put(ff, cptff);
            }
            Vector<String> tagSpace = cptoo.getTagSpace();
            for (String tag : tagSpace)
            {
                cptff.addTag(tag, cptoo.getProbNonLog(tag));
            }
        }
        m_mapCPTFF = mapCPTFF;
        return m_mapCPTFF;
    }
    HashMap<String, CPT> m_mapCPTFO = null;

    public HashMap<String, CPT> constructCPTFO()
    {
        if (m_mapCPTFO != null)
        {
            return m_mapCPTFO;
        }
        HashMap<String, CPT> mapCPTOO = constructCPTOO();
        HashMap<String, CPT> mapCPTFO = new HashMap<String, CPT>();
        Set<String> keys = mapCPTOO.keySet();
        for (String oo : keys)
        {
            CPT cptoo = mapCPTOO.get(oo);
            String fo = m_mapOF.get(oo.split(" ")[0]) + " " + oo.split(" ")[1];
            CPT cptfo = mapCPTFO.get(fo);
            if (cptfo == null)
            {
                cptfo = new CPT(fo);
                mapCPTFO.put(fo, cptfo);
            }
            Vector<String> tagSpace = cptoo.getTagSpace();
            for (String tag : tagSpace)
            {
                cptfo.addTag(tag, cptoo.getProbNonLog(tag));
            }
        }
        m_mapCPTFO = mapCPTFO;
        return m_mapCPTFO;
    }
    HashMap<String, CPT> m_mapCPTOF = null;

    public HashMap<String, CPT> constructCPTOF()
    {
        if (m_mapCPTOF != null)
        {
            return m_mapCPTOF;
        }
        HashMap<String, CPT> mapCPTOO = constructCPTOO();
        HashMap<String, CPT> mapCPTOF = new HashMap<String, CPT>();
        Set<String> keys = mapCPTOO.keySet();
        int cnt = 0;
        for (String oo : keys)
        {
        	System.out.println("oo["+ ++cnt +"]="+oo);
            CPT cptoo = mapCPTOO.get(oo);
            String of = oo.split(" ")[0] + " " + m_mapOF.get(oo.split(" ")[1]);
            System.out.println(of);
            CPT cptof = mapCPTOF.get(of);
            if (cptof == null)
            {
                cptof = new CPT(of);
                mapCPTOF.put(of, cptof);
            }
            Vector<String> tagSpace = cptoo.getTagSpace();
            for (String tag : tagSpace)
            {
                cptof.addTag(tag, cptoo.getProbNonLog(tag));
            }
        }
        m_mapCPTOF = mapCPTOF;
        return m_mapCPTOF;
    }
    HashMap<String, CPT> m_mapCPTFF1 = null;

    public HashMap<String, CPT> constructCPTFF1()
    {
        if (m_mapCPTFF1 != null)
        {
            return m_mapCPTFF1;
        }
        HashMap<String, CPT> mapCPTOO = constructCPTOO();
        HashMap<String, CPT> mapCPTFF1 = new HashMap<String, CPT>();
        Set<String> keys = mapCPTOO.keySet();
        for (String oo : keys)
        {
            CPT cptoo = mapCPTOO.get(oo);
            String ff1 = m_mapOF.get(oo.split(" ")[0]) + " " + m_mapOF.get(oo.split(" ")[1]) + "1";
            CPT cptff1 = mapCPTFF1.get(ff1);
            if (cptff1 == null)
            {
                cptff1 = new CPT(ff1);
                mapCPTFF1.put(ff1, cptff1);
            }
            Vector<String> tagSpace = cptoo.getTagSpace();
            for (String tag : tagSpace)
            {
                cptff1.addTag(tag.split(" ")[0], cptoo.getProbNonLog(tag));
            }
        }
        m_mapCPTFF1 = mapCPTFF1;
        return m_mapCPTFF1;
    }
    HashMap<String, CPT> m_mapCPTFF2 = null;

    public HashMap<String, CPT> constructCPTFF2()
    {
        if (m_mapCPTFF2 != null)
        {
            return m_mapCPTFF2;
        }
        HashMap<String, CPT> mapCPTOO = constructCPTOO();
        HashMap<String, CPT> mapCPTFF2 = new HashMap<String, CPT>();
        Set<String> keys = mapCPTOO.keySet();
        for (String oo : keys)
        {
            CPT cptoo = mapCPTOO.get(oo);
            String ff2 = m_mapOF.get(oo.split(" ")[0]) + " " + m_mapOF.get(oo.split(" ")[1]) + "2";
            CPT cptff2 = mapCPTFF2.get(ff2);
            if (cptff2 == null)
            {
                cptff2 = new CPT(ff2);
                mapCPTFF2.put(ff2, cptff2);
            }
            Vector<String> tagSpace = cptoo.getTagSpace();
            for (String tag : tagSpace)
            {
                cptff2.addTag(tag.split(" ")[1], cptoo.getProbNonLog(tag));
            }
        }
        m_mapCPTFF2 = mapCPTFF2;
        return m_mapCPTFF2;
    }
    HashMap<String, CPT> m_mapCPTOO1 = null;

    public HashMap<String, CPT> constructCPTOO1()
    {
        if (m_mapCPTOO1 != null)
        {
            return m_mapCPTOO1;
        }
        HashMap<String, CPT> mapCPTOO = constructCPTOO();
        HashMap<String, CPT> mapCPTOO1 = new HashMap<String, CPT>();
        Set<String> keys = mapCPTOO.keySet();
        for (String oo : keys)
        {
            CPT cptoo = mapCPTOO.get(oo);
            CPT cptoo1 = mapCPTOO1.get(oo);
            if (cptoo1 == null)
            {
                cptoo1 = new CPT(oo);
                mapCPTOO1.put(oo, cptoo1);
            }
            Vector<String> tagSpace = cptoo.getTagSpace();
            for (String tag : tagSpace)
            {
                cptoo1.addTag(tag.split(" ")[0], cptoo.getProbNonLog(tag));
            }
        }
        m_mapCPTOO1 = mapCPTOO1;
        return m_mapCPTOO1;
    }
    HashMap<String, CPT> m_mapCPTOO2 = null;

    public HashMap<String, CPT> constructCPTOO2()
    {
        if (m_mapCPTOO2 != null)
        {
            return m_mapCPTOO2;
        }
        HashMap<String, CPT> mapCPTOO = constructCPTOO();
        HashMap<String, CPT> mapCPTOO2 = new HashMap<String, CPT>();
        Set<String> keys = mapCPTOO.keySet();
        for (String oo : keys)
        {
            CPT cptoo = mapCPTOO.get(oo);
            CPT cptoo2 = mapCPTOO2.get(oo);
            if (cptoo2 == null)
            {
                cptoo2 = new CPT(oo);
                mapCPTOO2.put(oo, cptoo2);
            }
            Vector<String> tagSpace = cptoo.getTagSpace();
            for (String tag : tagSpace)
            {
                cptoo2.addTag(tag.split(" ")[1], cptoo.getProbNonLog(tag));
            }
        }
        m_mapCPTOO2 = mapCPTOO2;
        return m_mapCPTOO2;
    }

    public void printStatistics()
    {
        System.out.println("Features: " + getEmpCPTs().size());
        System.out.println("Sentences: " + getNumSentences());
        System.out.println("SSFF: " + getNumDiffFF());
        if (Global.g_bWithStartEndSymbols)
        {
            System.out.println("Tags (including start and end tag): " + getNumDiffTags());
        }
        else
        {
            System.out.println("Tags: " + getNumDiffTags());
        }
    }
    //different FF
    private int m_numDiffFF = -1;

    public int getNumDiffFF()
    {
        if (m_numDiffFF != -1)
        {
            return m_numDiffFF;
        }
        HashMap<String, Boolean> map = new HashMap<String, Boolean>();
        for (Sentence s : m_sentences)
        {
            for (int i = 0; i < s.size() - 1; ++i)
            {
                String ff = s.getFeature(i) + s.getFeature(i + 1);
                if (!map.containsKey(ff))
                {
                    map.put(ff, true);
                }
            }
        }
        m_numDiffFF = map.size();
        return m_numDiffFF;
    }
    //different SSFF
    private int m_numDiffSSFF = -1;

    public int getNumDiffSSFF()
    {
        if (m_numDiffSSFF != -1)
        {
            return m_numDiffSSFF;
        }
        HashMap<String, Boolean> map = new HashMap<String, Boolean>();
        for (Sentence s : m_sentences)
        {
            for (int i = 0; i < s.size() - 1; ++i)
            {
                String ssff = s.getTag(i) + s.getTag(i + 1) + s.getFeature(i) + s.getFeature(i + 1);
                if (!map.containsKey(ssff))
                {
                    map.put(ssff, true);
                }
            }
        }
        m_numDiffSSFF = map.size();
        return m_numDiffSSFF;
    }
    //different words
    private int m_numDiffTags = -1;

    public int getNumDiffTags()
    {
        if (m_numDiffTags != -1)
        {
            return m_numDiffTags;
        }
        HashMap<String, Boolean> map = new HashMap<String, Boolean>();
        for (Sentence s : m_sentences)
        {
            for (int i = 0; i < s.size(); ++i)
            {
                String tag = s.getTag(i);
                if (!map.containsKey(tag))
                {
                    map.put(tag, true);
                }
            }
        }
        m_numDiffTags = map.size();
        return m_numDiffTags;
    }
    //different words map
    private HashMap<String, Boolean> m_mapWords;

    public HashMap<String, Boolean> getMapWords()
    {
        if (m_mapWords != null)
        {
            return m_mapWords;
        }
        m_mapWords = new HashMap<String, Boolean>();
        for (Sentence s : m_sentences)
        {
            for (int i = 0; i < s.size(); ++i)
            {
                String word = s.getWord(i);
                if (!m_mapWords.containsKey(word))
                {
                    m_mapWords.put(word, true);
                }
            }
        }
        return m_mapWords;
    }

    public boolean isOOV(String word)
    {
        return !getMapWords().containsKey(word);
    }
    //different words
    private int m_numDiffWords = -1;

    public int getNumDiffWords()
    {
        if (m_numDiffWords != -1)
        {
            return m_numDiffWords;
        }
        HashMap<String, Boolean> map = new HashMap<String, Boolean>();
        for (Sentence s : m_sentences)
        {
            for (int i = 0; i < s.size(); ++i)
            {
                String word = s.getWord(i);
                if (!map.containsKey(word))
                {
                    map.put(word, true);
                }
            }
        }
        m_numDiffWords = map.size();
        return m_numDiffWords;
    }
    //total words or tags
    private int m_numTotalWords = -1;

    public int getNumTotalWords()
    {
        if (m_numTotalWords != -1)
        {
            return m_numTotalWords;
        }
        int num = 0;
        for (Sentence s : m_sentences)
        {
            num += s.size();
        }
        m_numTotalWords = num;
        return m_numTotalWords;
    }

    public Sentence getSentence(int i)
    {
        return m_sentences.get(i);
    }

    public int getNumSentences()
    {
        return m_sentences.size();
    }

    public Corpus genSubCorpus(int begin, int end)
    {
        Vector<Sentence> sentences = new Vector<Sentence>();
        for (int i = begin; i < end; ++i)
        {
            sentences.add(getSentence(i));
        }
        return new Corpus(sentences);
    }

    public void saveCorpus(String path)
    {
        StringBuilder sb = new StringBuilder();
        for (Sentence sentence : m_sentences)
        {
            sb.append(sentence.toString() + "\n");
        }
        Global.writeOutput(sb.toString(), path);
    }

    public void transform2MALLET(String path)
    {
        StringBuilder sb = new StringBuilder();
        for (Sentence s : m_sentences)
        {
            for (int i = 0; i < s.size(); ++i)
            {
                String word = s.getWord(i);
                String feature = m_mapOF.get(word).replace("_", " ");
                String pos = s.getTag(i);
                sb.append(pos + " ---- " + word + " " + feature + "\n");
            }
            sb.append("\n");
        }
        Global.writeOutput(sb.toString(), path);
    }

    public void transform2MALLET_more1(String path)
    {
        StringBuilder sb = new StringBuilder();
        for (Sentence s : m_sentences)
        {
            if (s.size() <= 1)
            {
                continue;
            }
            for (int i = 0; i < s.size(); ++i)
            {
                String word = s.getWord(i);
                String feature = m_mapOF.get(word).replace("_", " ");
                String pos = s.getTag(i);
                sb.append(pos + " ---- " + word + " " + feature + "\n");
            }
            sb.append("\n");
        }
        Global.writeOutput(sb.toString(), path);
    }
}
