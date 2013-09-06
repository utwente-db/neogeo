package nl.utwente.db.ZehminCRF.corpus;

import java.util.Vector;

import nl.utwente.db.ZehminCRF.utils.Global;

/**
 * @author Zhemin Zhu Created on Jul 9, 2012
 *
 * CTIT Database Group Universiteit Twente
 */
public class Sentence
{

    private Vector<String> m_words;
    private Vector<String> m_fe;
    private Vector<String> m_tags;

    public Sentence(String strSentence)
    {
        m_words = new Vector<String>();
        m_fe = new Vector<String>();
        m_tags = new Vector<String>();

        String[] lines = strSentence.split("\n");
        
        for (String line : lines)
        {
            
            String[] strs = line.split("\t");
            m_words.add(strs[0]);
            String fe = new String();
            for (int i = 1; i < strs.length - 1; ++i)
            {
                fe += strs[i] + "_";
            }
            m_fe.add(fe.substring(0, fe.length() - 1));
            m_tags.add(strs[strs.length - 1]);
        }

        if (Global.g_bWithStartEndSymbols)
        {
            Global.addStartEndWords(m_words);
            Global.addStartEndFetures(m_fe);
            Global.addStartEndTags(m_tags);
        }
    }

    public Sentence(
            Vector<String> words,
            Vector<String> fe,
            Vector<String> tags)
    {
        m_words = words;
        m_fe = fe;
        m_tags = tags;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size(); ++i)
        {
            sb.append(getWord(i) + "\t");
            sb.append(getFeature(i).replaceAll("_", "\t") + "\t");
            sb.append(getTag(i) + "\n");
        }
        return sb.toString();
    }

    public Sentence genSubSentence(int start, int end)
    {
        Vector<String> words = new Vector<String>();
        Vector<String> fe = new Vector<String>();
        Vector<String> tags = new Vector<String>();
        for (int i = start; i < end; ++i)
        {
            words.add(getWord(i));
            fe.add(getFeature(i));
            tags.add(getTag(i));
        }
        return new Sentence(words, fe, tags);
    }

    public int size()
    {
        return m_words.size();
    }

    public String getWord(int i)
    {
        return m_words.get(i);
    }

    public String getFeature(int i)
    {
        return m_fe.get(i);
    }

    public String getTag(int i)
    {
        return m_tags.get(i);
    }

    public Vector<String> getColumn(int i)
    {
        Vector<String> vec = new Vector<String>();
        vec.add(m_words.get(i));
        vec.add(m_fe.get(i));
        vec.add(m_tags.get(i));
        return vec;
    }

    public void printTags()
    {
        StringBuilder sb = new StringBuilder();
        for (String tag : m_tags)
        {
            sb.append(tag + " ");
        }
        System.out.println(sb.toString().trim());
    }
}
