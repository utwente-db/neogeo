/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import nl.utwente.db.ZehminCRF.corpus.Corpus;
import nl.utwente.db.ZehminCRF.main.Decoder;
import nl.utwente.db.ZehminCRF.sp.CRModel_sp1;
import nl.utwente.db.ZehminCRF.utils.Global;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 *
 * @author badiehm
 */
public class EntityResolver
{
	public static boolean verbose = true;
	
	private static final String CONFIG_FILENAME = "database.properties";

	public static Connection getGeonamesConnection(){
		String hostname = null;
		String port = null;
		String username = null;
		String password = null;
		String database = null; 
		
		Properties prop = new Properties();
		try {
			InputStream is =
					(new EntityResolver()).getClass().getClassLoader().getResourceAsStream(CONFIG_FILENAME);
			prop.load(is);
			hostname = prop.getProperty("hostname");
			port = prop.getProperty("port");
			username = prop.getProperty("username");
			password = prop.getProperty("password");
			database = prop.getProperty("database");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return null;
		}
		if ( verbose )
			System.out.println("PostgreSQL JDBC Driver Registered!");
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(
					"jdbc:postgresql://"+hostname+":"+port+"/"+database, username, password);
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;

		}
		if (connection != null) {
			if ( verbose )
				System.out.println("You made it, take control your database now!");
		} else {
			throw new RuntimeException("Failed to make connection!");
		}
		return connection;
	}
	
	// connection should also be visible in other packages
	public static Connection geonames_conn = getGeonamesConnection();

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args)
    {
    	//String TweetStr = "Onderweg naar Enschede voor hopelijk een mooi feestje vanavond. #batavieren";
        //String TweetStr = "Campuspop:Alle Batavieren kunnen morgen voor 16 Euro kaarten kopen voor Campuspop met Anouk, Candy Dulfer, Ben Saundersenz.#batavierenrace";
        String TweetStr="Niks te doen dit weekend? Festival GOGBOT in Enschede (Sciencefiction, technologie, #robots) http://www.fantasymedia.nl/content/festival-gogbot-2013-enschede-sciencefiction-technologie-robots?utm_source=twitterfeed&utm_medium=twitter … http://2013.gogbot.nl";

    	resolveEntity(TweetStr, "nl");
    }
    
	public static Vector<NamedEntity> resolveEntity(String TweetStr, String lang) {
		// TODO: make this work both for "nl" end "en" language
		// PrepareTrainingFile();
		List<Token> TokenList = PrepareTestFile_StanfordTokenizer(TweetStr);
		// PrepareTestFile_JavaTokenizer(TweetStr);

		Corpus train_corpus = new Corpus("crfTrain.txt");
		// System.out.println("#Training Sentences: " +
		// train_corpus.getNumSentences());

		Corpus test_corpus = new Corpus("crfTest.txt");
		// System.out.println("#Test Sentences: " +
		// test_corpus.getNumSentences());

		CRModel_sp1 model = new CRModel_sp1(train_corpus, "CRF.model", false);

		List<NamedEntity> NEs = new Decoder(train_corpus, test_corpus, model)
				.decode(TokenList, TweetStr);

		Vector<NamedEntity> res = new Vector<NamedEntity>();
		for (int i = 0; i < NEs.size(); i++) {
			NamedEntity entity = NEs.get(i);
			try {
				PreparedStatement ps = geonames_conn
						.prepareStatement("select name,latitude,longitude,country,alternatenames,population,elevation,fclass from geoname where lower(name) = ?;");
				String candidate = entity.getMention().toLowerCase();
				ps.setString(1, candidate);
				ResultSet rs = ps.executeQuery();
				// System.out.println("XX="+ps);
				while (rs.next()) {
					GeoEntity ge = new GeoEntity(entity, rs.getDouble("latitude"),rs.getDouble("longitude"),rs.getString("country"),rs.getString("alternatenames"),rs.getInt("population"),rs.getInt("elevation"),rs.getString("fclass"));
					if ( true )
						System.out.println("RESOLVED[" + ge + "]");
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (entity.isResolved())
				res.add(entity);
			else {
				if ( verbose )
					System.out.println("UNRESOLVED["+entity+"]");
			}
		}
		return res;
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
