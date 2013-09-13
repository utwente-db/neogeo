/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.PTBTokenizerAnnotator;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.time.TimeAnnotations;
import edu.stanford.nlp.util.CoreMap;
import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import nl.utwente.db.ZehminCRF.corpus.Corpus;
import nl.utwente.db.ZehminCRF.main.Decoder;
import nl.utwente.db.ZehminCRF.sp.CRModel_sp1;
import nl.utwente.db.ZehminCRF.utils.Global;

/**
 *
 * @author badiehm
 */
public class TEC4SE_Ver1
{

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args)
    {
       // String TweetStr = "Onderweg naar Enschede voor hopelijk een mooi feestje vanavond. #batavieren";
        //String TweetStr = "Campuspop:Alle Batavieren kunnen morgen voor 16 Euro kaarten kopen voor Campuspop met Anouk, Candy Dulfer, Ben Saundersenz.#batavierenrace";
        String TweetStr="Niks te doen dit weekend? Festival GOGBOT in Enschede (Sciencefiction, technologie, #robots) http://www.fantasymedia.nl/content/festival-gogbot-2013-enschede-sciencefiction-technologie-robots?utm_source=twitterfeed&utm_medium=twitter … http://2013.gogbot.nl";

        PrepareTrainingFile();
        List<Token> TokenList = PrepareTestFile_StanfordTokenizer(TweetStr);
        //PrepareTestFile_JavaTokenizer(TweetStr);

        Corpus train_corpus = new Corpus("crfTrain.txt");
        //System.out.println("#Training Sentences: " + train_corpus.getNumSentences());

        Corpus test_corpus = new Corpus("crfTest.txt");
        //System.out.println("#Test Sentences: " + test_corpus.getNumSentences());

        CRModel_sp1 model = new CRModel_sp1(train_corpus, "CRF.model", false);

        List<NamedEntity> NEs = new Decoder(train_corpus, test_corpus, model).decode(TokenList, TweetStr);

        for (int i = 0; i < NEs.size(); i++)
        {
            System.out.println(NEs.get(i).getOffset() + ":" + NEs.get(i).getMention() + "--->" + NEs.get(i).getTag());
        }
    }

    private static void PrepareTrainingFile()
    {
        try
        {
            String PathSource = "ned.train";
            String PathTarget = "crfTrain.txt";
            
            ClassLoader classLoader = new Global().getClass().getClassLoader();
            URL url = classLoader.getResource(PathSource);
            if (url == null)
            {
                throw new RuntimeException("Global:readFile: unable to locate resource: " + PathSource);
            }
            InputStreamReader isr = new InputStreamReader(new FileInputStream(url.getFile()), "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(isr);
            
            
            url = classLoader.getResource(PathTarget);
            if (url == null)
            {
                throw new RuntimeException("Global:readFile: unable to locate resource: " + PathSource);
            }
            FileOutputStream fos = new FileOutputStream(url.getFile());
            OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");
            
            
            String line = "";
            while ((line = bufferedReader.readLine()) != null)
            {

                System.out.println(line);

                if (!line.startsWith("-DOCSTART-"))
                {
                    if (line.length() != 0)
                    {
                        String[] tokens = line.split(" ");
                        String word = tokens[0];
                        String feature = "";
                        //feature = tokens[1];
                        if (Character.isUpperCase(word.charAt(0)))
                        {
                            feature = "t";
                        }
                        else
                        {
                            feature = "f";
                        }
                        String tag = tokens[2];
                        if (!tag.equalsIgnoreCase("O"))
                        {
                            tag = tag.substring(0,2)+"NE";
                        }
                        out.write(word + "\t" + feature + "\t" + tag);
                        if (bufferedReader.ready())
                        {
                            out.write("\n");
                        }
                    }
                    else
                    {
                        out.write("\n");
                    }
                }
            }
            out.close();
            fos.close();
            isr.close();
            bufferedReader.close();
        }
        catch (Exception ex)
        {
            Logger.getLogger(TEC4SE_Ver1.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private static void PrepareTestFile_JavaTokenizer(String TweetStr)
    {
        try
        {
            String PathTarget = "crfTest.txt";

            ClassLoader classLoader = new Global().getClass().getClassLoader();
            URL url = classLoader.getResource(PathTarget);
            if (url == null)
            {
                throw new RuntimeException("Global:readFile: unable to locate resource: " + PathTarget);
            }
            FileOutputStream fos = new FileOutputStream(url.getFile());
            OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");

            StringTokenizer ST = new StringTokenizer(TweetStr);
            while (ST.hasMoreTokens())
            {
                String Token = ST.nextToken();
                String feature = "";
                //feature = ??;
                if (Character.isUpperCase(Token.charAt(0)))
                {
                    feature = "t";
                }
                else
                {
                    feature = "f";
                }
                out.write(Token + "\t" + feature + "\t" + "O");
                if (ST.hasMoreTokens())
                {
                    out.write("\n");
                }
            }
            out.close();
            fos.close();
        }
        catch (Exception ex)
        {
            Logger.getLogger(TEC4SE_Ver1.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static List<Token> PrepareTestFile_StanfordTokenizer(String TweetStr)
    {
        List<Token> TokensList = new ArrayList<Token>();
        try
        {
            String PathTarget = "crfTest.txt";
            
            ClassLoader classLoader = new Global().getClass().getClassLoader();
            URL url = classLoader.getResource(PathTarget);
            if (url == null)
            {
                throw new RuntimeException("Global:readFile: unable to locate resource: " + PathTarget);
            }
            FileOutputStream fos = new FileOutputStream(url.getFile());
            OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");

            // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution 
            Properties props = new Properties();
            props.put("annotators", "tokenize, ssplit");
            StanfordCoreNLP pipeline = new StanfordCoreNLP(props);


            // create an empty Annotation just with the given text
            Annotation document = new Annotation(TweetStr);

            // run all Annotators on this text
            pipeline.annotate(document);

            // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(SentencesAnnotation.class);
            int sentenceSize = sentences.size();
            int sentenceCnt = 0;
            for (CoreMap sentence : sentences)
            {
                // traversing the words in the current sentence
                // a CoreLabel is a CoreMap with additional token-specific methods
                sentenceCnt++;
                List<CoreLabel> Tokens = sentence.get(TokensAnnotation.class);
                int tokenSize = Tokens.size();
                int tokenCnt = 0;
                for (CoreLabel token : Tokens)
                {
                    tokenCnt++;
                    String word = token.get(TextAnnotation.class);
                    int offset = token.beginPosition();
                    TokensList.add(new Token(word, offset));
                    String feature = "";
                    if (Character.isUpperCase(word.charAt(0)))
                    {
                        feature = "t";
                    }
                    else
                    {
                        feature = "f";
                    }
                    out.write(word + "\t" + feature + "\t" + "O");
                    if (!(sentenceCnt == sentenceSize && tokenCnt == tokenSize))
                    {
                        out.write("\n");
                    }
                }
            }

            out.close();
            fos.close();

        }
        catch (Exception ex)
        {
            Logger.getLogger(TEC4SE_Ver1.class.getName()).log(Level.SEVERE, null, ex);
        }
        return TokensList;
    }
}
