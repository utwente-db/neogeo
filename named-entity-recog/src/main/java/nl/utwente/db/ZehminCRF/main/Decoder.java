package nl.utwente.db.ZehminCRF.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import nl.utwente.db.ZehminCRF.corpus.Corpus;
import nl.utwente.db.ZehminCRF.corpus.Sentence;
import nl.utwente.db.ZehminCRF.sp.CRModel_sp1;
import nl.utwente.db.ZehminCRF.utils.Global;
import nl.utwente.db.ZehminCRF.utils.MyTimer;
import nl.utwente.db.ZehminCRF.viterbi.CRStepGain;
import nl.utwente.db.ZehminCRF.viterbi.Path;
import nl.utwente.db.ZehminCRF.viterbi.ViterbiAlg;
import nl.utwente.db.named_entity_recog.NamedEntity;
import nl.utwente.db.named_entity_recog.Token;

/**
 * @author Zhemin Zhu Created on Aug 22, 2012
 *
 * CTIT Database Group Universiteit Twente
 */
public class Decoder
{

    int m_correctTag = 0;
    int m_totalTag = 0;
    int m_correctSentence = 0;
    int m_totalSentence = 0;
    int m_correctOOV = 0;
    int m_totalOOV = 0;
    Corpus m_training_corpus;
    Corpus m_decoding_corpus;
    CRModel_sp1 m_model;
    ViterbiAlg m_va;
    MyTimer m_timer;

    public Decoder(Corpus training_corpus, Corpus decoding_corpus, CRModel_sp1 model)
    {
        m_training_corpus = training_corpus;
        m_decoding_corpus = decoding_corpus;
        m_model = model;
        m_va = new ViterbiAlg(
                new CRStepGain(model),
                model, false);
        m_timer = new MyTimer();
    }
    
/*
* TopK paths
*/
// public Vector<Path> getTopKPaths(Sentence s)
// {
// // Vector<Path> paths = m_model.getPromisingPaths(s);
// Path top1Tags = new Path(m_va.decode(s));
// if (paths.size() == 0 || !paths.firstElement().getPath().equals(top1Tags.getPath()))
// {
// // m_model.score(top1Tags, s);
// paths.add(0, top1Tags);
// }
// boolean bDuplicate = checkDuplicatePath(paths);
// if (bDuplicate)
// {
// //System.err.println("Same Path!!!");
// }
// return paths;
// }
    
        public boolean checkDuplicatePath(Vector<Path> paths)
    {
        HashMap<String, Boolean> map = new HashMap<String, Boolean>();
        for (Path path : paths)
        {
            String strPath = path.getPath();
            if (map.containsKey(strPath))
            {
                return true;
            }
            else
            {
                map.put(strPath, true);
            }
        }
        return false;
    }
        
// public void decodeTopK(int k)
// {
// m_timer.start();
// for (int i = 0; i < m_decoding_corpus.getNumSentences(); ++i)
// {
// Sentence sentence = m_decoding_corpus.getSentence(i);
// Vector<Path> paths = getTopKPaths(sentence);
// double precision = checkSentence(sentence, paths.firstElement().getTags());
// System.out.println("Sentence: " + i + "\t" + precision + "\t topK: " + paths.size());
// }
// m_timer.end();
// printPrecision();
// }

    public void decode()
    {
        m_timer.start();
        for (int i = 0; i < m_decoding_corpus.getNumSentences(); ++i)
        {
            Sentence sentence = m_decoding_corpus.getSentence(i);
            Vector<String> annotatedTags = m_va.decode(sentence);
            checkSentence(sentence, annotatedTags);
            //System.out.println("Sentence: " + i);
        }
        m_timer.end();
        printPrecision();
    }

    private double checkSentence(Sentence s, Vector<String> vecAnnTags)
    {
        int numCorrect = 0;
        if (s.size() != vecAnnTags.size())
        {
            System.err.println("Decoding Error: num of tags is not equal to num of observes!!");
        }

        if (Global.g_bWithStartEndSymbols)
        {
            s = s.genSubSentence(1, s.size() - 1);
            vecAnnTags = new Vector(vecAnnTags.subList(1, vecAnnTags.size() - 1));
        }

        ++m_totalSentence;
        boolean bCorrSentence = true;
        for (int i = 0; i < s.size(); ++i)
        {
            ++m_totalTag;
            boolean bOOV = m_training_corpus.isOOV(s.getWord(i));
            if (bOOV)
            {
                ++m_totalOOV;
            }
            if (s.getTag(i).equals(vecAnnTags.get(i)))
            {
                ++m_correctTag;
                ++numCorrect;
                if (bOOV)
                {
                    ++m_correctOOV;
                }
            }
            else
            {
                bCorrSentence = false;
                //System.out.println(s.getWord(i) + ": " + vecAnnTags.get(i) + "=>" + s.getPos(i));
            }
        }
        if (bCorrSentence)
        {
            ++m_correctSentence;
        }

        return numCorrect * 1.0 / vecAnnTags.size();
    }

    private void printPrecision()
    {
        m_timer.printTime();

        System.out.println("Total Tags: " + m_totalTag
                + "\t Correct Tag: " + m_correctTag
                + "\t Precision: " + 1.0 * m_correctTag / m_totalTag);

        System.out.println("Total Sentences: " + m_totalSentence
                + "\t Correct Sentences: " + m_correctSentence
                + "\t Precision: " + 1.0 * m_correctSentence / m_totalSentence);

        System.out.println("Total OOVs: " + m_totalOOV
                + "\t Correct OOVs: " + m_correctOOV
                + "\t Precision: " + 1.0 * m_correctOOV / m_totalOOV);

        System.out.println("Total non-OOVs: " + (m_totalTag - m_totalOOV)
                + "\t Correct non-OOVs: " + (m_correctTag - m_correctOOV)
                + "\t Precision: " + 1.0 * (m_correctTag - m_correctOOV) / (m_totalTag - m_totalOOV));

    }
    
    // public List<NamedEntity> decodeTopK(String tweetStr, int TopK)
// {
// List<NamedEntity> NEsList=new ArrayList<NamedEntity>();
// Sentence sentence = m_decoding_corpus.getSentence(0);
//
// //Vector<String> annotatedTags = m_va.decode(sentence);
// Vector<Path> paths = getTopKPaths(sentence);
// int size = TopK;
// if (paths.size() < size)
// {
// size = paths.size();
// }
// for (int i = 0; i < size; i++)
// {
// Path path = paths.get(i);
// Vector<String> annotatedTags = path.getTags();
// double score = path.getScore();
// checkSentence(sentence, tweetStr, annotatedTags, NEsList);
//
// }
// return NEsList;
// }

    public List<NamedEntity> decode(List<Token> TokenList,String TweetStr)
    {
        List<NamedEntity> NEsList = new ArrayList<NamedEntity>();
        Sentence sentence = m_decoding_corpus.getSentence(0);

        Vector<String> annotatedTags = m_va.decode(sentence);
        checkSentence(sentence, TokenList,TweetStr, annotatedTags, NEsList);

        return NEsList;
    }

    private void checkSentence(Sentence s, List<Token> TokenList,String TweetStr, Vector<String> vecAnnTags, List<NamedEntity> NEsList)
    {
        if (s.size() != vecAnnTags.size())
        {
            System.err.println("Decoding Error: num of tags is not equal to num of observes!!");
        }

        if (Global.g_bWithStartEndSymbols)
        {
            s = s.genSubSentence(1, s.size() - 1);
            vecAnnTags = new Vector(vecAnnTags.subList(1, vecAnnTags.size() - 1));
        }


        String TempNEStr = "";
        String TempNETag = "";
        int NEStartOffset=-1;
        int NEEndOffset=-1;
        for (int i = 0; i < s.size(); i++)
        {
            String token = s.getWord(i);
            int TokenStartOffset=TokenList.get(i).getOffset();
            String ResultTag = vecAnnTags.get(i);

            if (!ResultTag.equalsIgnoreCase("o"))
            {
                if (ResultTag.startsWith("B"))
                {
                    if (TempNEStr.isEmpty())
                    {
                        TempNEStr = token;
                        NEStartOffset=TokenStartOffset;
                        NEEndOffset=TokenStartOffset+token.length();
                        TempNETag = ResultTag.substring(2);
                    }
                    else
                    {                        
                        NEsList.add(new NamedEntity(TweetStr.substring(NEStartOffset, NEEndOffset), TempNETag, NEStartOffset, 0));
                        TempNEStr = token;
                        NEStartOffset=TokenStartOffset;
                        NEEndOffset=TokenStartOffset+token.length();
                        TempNETag = ResultTag.substring(2);
                    }
                }
                else if (ResultTag.startsWith("I"))
                {
                    if(TempNEStr.isEmpty())
                    {
                        System.out.println("Strange to have "+ResultTag+" as a first tag!!");
                        NEStartOffset=TokenStartOffset;
                    }
                    else
                    {
                        TempNEStr += " ";                        
                    }
                    TempNEStr += token;
                    NEEndOffset=TokenStartOffset+token.length();
                    TempNETag = ResultTag.substring(2);
                }

            }
            else if (ResultTag.equalsIgnoreCase("o"))
            {
                if (!TempNEStr.isEmpty())
                {
                    NEsList.add(new NamedEntity(TweetStr.substring(NEStartOffset, NEEndOffset), TempNETag, NEStartOffset, 0));
                    NEStartOffset=-1;
                    NEEndOffset=-1;
                    TempNEStr = "";
                    TempNETag = "";
                }
            }
        }
        if (!TempNEStr.isEmpty())
        {
            NEsList.add(new NamedEntity(TweetStr.substring(NEStartOffset, NEEndOffset), TempNETag, NEStartOffset, 0));
            NEStartOffset=-1;
            NEEndOffset=-1;
            TempNEStr = "";
            TempNETag = "";
        }
    }

    public static void main(String[] strs)
    {
        //this line is for fixing the JDK bug.
        //this bug happens in the method Collections.sort().
        System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
        //Brown Corpus

        /*
         * Corpus corpus = new Corpus(Global.g_BrwonCorpusFile);
         * //training_corpus.printStatistics(); Corpus training_corpus =
         * corpus.genSubCorpus(0, corpus.getNumSentences() - 1000); Corpus
         * decoding_corpus = corpus.genSubCorpus(corpus.getNumSentences() -
         * 1000, corpus.getNumSentences());
         *
         *
         * CRModel_sp1 model = new CRModel_sp1(training_corpus,
         * Global.g_BrwonModelFile, false); new Decoder(training_corpus,
         * decoding_corpus, model).decode();
         */

        //new Decoder(training_corpus, decoding_corpus, model).decode();


        //OOF is not perfect. the joint feature is OOV but the partial feature may be not
        //fix it.

        Corpus train_corpus = new Corpus(null,Global.g_desktop_windows + "CRFTrainingFile.txt",false);
        System.out.println("#Training Sentences: " + train_corpus.getNumSentences());

        Corpus test_corpus = new Corpus(null,Global.g_desktop_windows + "CRFTestFile.txt",false);
        System.out.println("#Test Sentences: " + test_corpus.getNumSentences());


        //test_corpus = test_corpus.genSubCorpus(begin, end);
        CRModel_sp1 model = new CRModel_sp1(train_corpus, Global.g_BrwonModelFile, false);
        new Decoder(train_corpus, test_corpus, model).decode();



        /*
         * Corpus train_corpus = new
         * Corpus("D:/Profiles/ZhuZ/Desktop/WSJ_pos/WSJ_pos.train.tran");
         * System.out.println("#Training Sentences: " +
         * train_corpus.getNumSentences());
         *
         * Corpus dev_corpus = new
         * Corpus("D:/Profiles/ZhuZ/Desktop/WSJ_pos/WSJ_pos.dev.tran");
         * System.out.println("#Training Sentences: " +
         * train_corpus.getNumSentences());
         *
         * Corpus test_corpus = new
         * Corpus("D:/Profiles/ZhuZ/Desktop/WSJ_pos/WSJ_pos.test.tran");
         * System.out.println("#Test Sentences: " +
         * test_corpus.getNumSentences());
         *
         * train_corpus = train_corpus.merge(dev_corpus);
         *
         * //test_corpus = test_corpus.genSubCorpus(begin, end); CRModel_sp1
         * model = new CRModel_sp1(train_corpus, Global.g_BrwonModelFile,
         * false); new Decoder(train_corpus, test_corpus, model).decode();
         */


        /*
         * Corpus train_corpus = new
         * Corpus("D:/Profiles/ZhuZ/Desktop/NER/NER_trans/ned.train");
         * System.out.println("#Training Sentences: " +
         * train_corpus.getNumSentences());
         *
         * Corpus dev_corpus = new
         * Corpus("D:/Profiles/ZhuZ/Desktop/NER/NER_trans/ned.testa");
         * System.out.println("#Development Sentences: " +
         * train_corpus.getNumSentences());
         *
         * Corpus test_corpus = new
         * Corpus("D:/Profiles/ZhuZ/Desktop/NER/NER_trans/ned.testb");
         * System.out.println("#Test Sentences: " +
         * test_corpus.getNumSentences());
         *
         * //test_corpus = test_corpus.genSubCorpus(begin, end); CRModel_sp1
         * model = new CRModel_sp1(train_corpus, Global.g_BrwonModelFile,
         * false); new Decoder(train_corpus, test_corpus, model).decode();
         */





        //NER Corpus
		/*
         * Corpus training_corpus = new Corpus(Global.g_desktop_windows +
         * "NER/NER_trans/ned.train"); Corpus decoding_corpus = new
         * Corpus(Global.g_desktop_windows + "NER/NER_trans/ned.testb"); CRModel
         * model = new CRModel(training_corpus, Global.g_desktop_windows +
         * "NER/NER_trans/ner_model", false); new Decoder(training_corpus,
         * decoding_corpus, model).decode();
         */
    }
}
