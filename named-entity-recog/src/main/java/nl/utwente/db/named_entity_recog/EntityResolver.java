/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import nl.utwente.db.ZehminCRF.corpus.Corpus;
import nl.utwente.db.ZehminCRF.corpus.Sentence;
import nl.utwente.db.ZehminCRF.main.Decoder;
import nl.utwente.db.ZehminCRF.sp.CRModel_sp1;
import nl.utwente.db.neogeo.utils.FileUtils;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 *
 * @author badiehm & flokstra
 */
public class EntityResolver
{

    /*
     * For Mena: - I Changed the GeoEntity selection for names.length > 2 I
     * added a hashtag tokenizer to tokenize strings like #enschede
     */
    public static boolean verbose = true;
    public static boolean isTrained = false;

    //private static final String TRAINING_DIRECTORY = "/home/flokstra/crf/train/";
    private static final String TRAINING_DIRECTORY = "/Users/flokstra/crf/train/";
    //private static final String TRAINING_DIRECTORY = "F:/Projects/neogeo/named-entity-recog/";
    
    private String lang;
    private List<Token> TokenList = null;
    private Vector<Sentence> CRFsentences = null;
    // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution 
    private Properties props = null;
    private StanfordCoreNLP pipeline = null;
    private Corpus train_corpus = null;
    private CRModel_sp1 model = null;
    Connection c; // geonames db connection

    public EntityResolver(Connection c, String lang)
    {
        this.c = c;
        this.lang = lang;
        Initialize();
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
    	resolveSingle();
    	// resolveFromDB();
    }
    
    public static void resolveSingle()
    {
        // String TweetStr_nl = "Onderweg naar Enschede voor hopelijk een mooi feestje vanavond. #batavieren";
        //String TweetStr_nl = "Ik zit op de fiets van Enschede naar Almelo dwars door Hengelo denkend aan #Nepal.";
        String TweetStr_nl = "Ik zit op de fiets van Den Haag naar Den Bosch, door Bad Bentheim denkend aan Nepal.";
        //String TweetStr = "Campuspop:Alle Batavieren kunnen morgen voor 16 Euro kaarten kopen voor Campuspop met Anouk, Candy Dulfer, Ben Saundersenz.#batavierenrace";
        //String TweetStr = "Niks te doen dit weekend? Festival GOGBOT in Enschede (Sciencefiction, technologie, #robots) http://www.fantasymedia.nl/content/festival-gogbot-2013-enschede-sciencefiction-technologie-robots?utm_source=twitterfeed&utm_medium=twitter … http://2013.gogbot.nl";
        String TweetStr_en = "Some pictures of our show in Enschede by Paul Bergers";


        try
        {
            // resolveEntity(TweetStr_en, "en");
            //resolveEntity(TweetStr_nl, "nl");
            EntityResolver er = new EntityResolver(GeoNamesDB.getConnection(),"nl");
            for (int i = 0; i < 1; i++)
            {
                er.resolveEntityFastTimed(TweetStr_nl);
            }
        }
        catch (SQLException e)
        {
            System.out.println("#!CAUGHT: " + e);
            e.printStackTrace();
        }
    }
    
    public static void resolveFromDB()
    {
		try {
			TestTweetTable ttt = new TestTweetTable( GeoNamesDB.getConnection() );
			
			if ( true ) {
				ResultSet rs = ttt.startTestTweets(TestTweetTable.tttTable);
				
				while( rs.next() ) {
					String tweet = rs.getString(2);
					
					System.out.println(tweet);
				}
				ttt.stopTestTweets(rs);
			}
		} catch (Exception e) {
			System.out.println("#CAUGHT: "+e);
			e.printStackTrace();
		}
    }

    public Vector<NamedEntity> resolveEntityFastTimed(String TweetStr) throws SQLException
    {
        long startTime = System.nanoTime();
        Vector<NamedEntity> res = resolveEntity(TweetStr);
        long endTime = System.nanoTime();

        long duration = endTime - startTime;
        System.out.println("time[" + duration / 1000000 + "msec]: " + TweetStr);

        return res;
    }

    public Vector<NamedEntity> resolveEntity(String TweetStr) throws SQLException
    {
        if (verbose)
        {
            System.out.println("resolveEntity: tweet=" + TweetStr);
            System.out.println("resolveEntity: lang=" + lang);
        }
        if (!"nl".equals(lang))
        {
            lang = "en";
        }
        if (verbose)
        {
            System.out.println("#!EntityResolver.resolveEntity() called.");
        }

        TokenList.clear();
        CRFsentences.clear();
        
        PrepareTestFile_StanfordTokenizer(TweetStr, TokenList, CRFsentences);
       
        Corpus test_corpus = new Corpus(CRFsentences);
        
        List<NamedEntity> NEs = new Decoder(train_corpus, test_corpus, model).decode(TokenList, TweetStr);

        Vector<NamedEntity> res = new Vector<NamedEntity>();
        for (int i = 0; i < NEs.size(); i++)
        {
            NamedEntity entity = NEs.get(i);
            
            PreparedStatement ps = c.prepareStatement("select name,latitude,longitude,country,alternatenames,population,elevation,fclass from geoname where lower(name) = ?;");
            String candidate = entity.getMention().toLowerCase();
            ps.setString(1, candidate);
            ResultSet rs = ps.executeQuery();
            while (rs.next())
            {
                GeoEntity ge = new GeoEntity(entity, rs.getDouble("latitude"),
                        rs.getDouble("longitude"), rs.getString("country"),
                        rs.getString("alternatenames"),
                        rs.getInt("population"), rs.getInt("elevation"),
                        rs.getString("fclass"));
                if (verbose)
                {
                    System.out.println("RESOLVED[" + ge + "]");
                }
            }

            if (entity.isResolved())
            {
                res.add(entity);
            }
            else
            {
                if (verbose)
                {
                    System.out.println("UNRESOLVED[" + entity + "]");
                }
            }
        }
        return res;
    }
    private final static String traindir = TRAINING_DIRECTORY;

    private void PrepareTrainingFile(String lang)
    {
        try
        {
            String PathSource = "";
            int tagIndex = 0;
            if ("nl".equalsIgnoreCase(lang))
            {
                PathSource = "ned.train";
                tagIndex = 2;
            }
            else
            {
                PathSource = "eng.train";
                tagIndex = 3;
            }
            String PathTarget = TRAINING_DIRECTORY + "crfTrain_" + lang.toLowerCase() + ".txt";

            InputStream is = null;
            String trainfile = null;
            try
            {
                trainfile = traindir + PathSource;
                is = new FileInputStream(trainfile);
                if (verbose)
                {
                    System.out.println("#!opened training file: " + trainfile);
                }
            }
            catch (IOException ignore_e)
            {
                if (verbose)
                {
                    System.out.println("#!fail open training file " + trainfile + ", try get it from resources");
                }
                String str = FileUtils.getFileAsString(PathSource);
                is = new ByteArrayInputStream(str.getBytes());
                if (verbose)
                {
                    System.out.println("#!opened training file from resources: " + PathSource);
                }
            }
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));

            FileOutputStream fos = new FileOutputStream(PathTarget);
            OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");

            int count = 0;
            String line = "";
            while ((line = bufferedReader.readLine()) != null)
            {
                count++;
                // System.out.println(line);

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
                        String tag = tokens[tagIndex];
                        if (!tag.equalsIgnoreCase("O"))
                        {
                            tag = tag.substring(0, 2) + "NE";
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
                else
                {
                    if (!lang.equalsIgnoreCase("nl"))
                    {
                        line = bufferedReader.readLine();
                    }
                }
            }
            if (verbose)
            {
                System.out.println("#!PrepareTrainingFile: convert " + count + " lines from " + PathSource + " into " + PathTarget + ".");
            }
            out.close();
            fos.close();
            // isr.close();
            bufferedReader.close();
            // System.exit(0);
        }
        catch (Exception ex)
        {
            Logger.getLogger(TEC4SE_Ver1.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private void PrepareTestFile_StanfordTokenizer(String TweetStr, List<Token> TokensList, Vector<Sentence> CRFsentences)
    {
        TokensList.clear();
        StringBuffer SB = new StringBuffer();
        try
        {
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
                    SB.append(word + "\t" + feature + "\t" + "O");
                    if (!(sentenceCnt == sentenceSize && tokenCnt == tokenSize))
                    {
                        SB.append("\n");
                    }
                }
            }
            Sentence CRFsentence = new Sentence(SB.toString());
            CRFsentences.add(CRFsentence);

        }
        catch (Exception ex)
        {
            Logger.getLogger(EntityResolver.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void Initialize()
    {
        if (!isTrained)
        {
            PrepareTrainingFile("nl");
            PrepareTrainingFile("en");
            isTrained = true;
        }

        TokenList = new ArrayList<Token>();
        CRFsentences = new Vector<Sentence>();

        train_corpus = new Corpus(TRAINING_DIRECTORY, "crfTrain_" + lang.toLowerCase() + ".txt", true);
        model = new CRModel_sp1(train_corpus, "CRF.model", false);    

        props = new Properties();
        props.put("annotators", "tokenize, ssplit");
        pipeline = new StanfordCoreNLP(props);
    }
}
