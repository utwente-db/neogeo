/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.tweet_text_classifier;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Hashtable;
import nl.utwente.db.ZehminCRF.utils.Global;
import nl.utwente.db.neogeo.twitter.Tweet;
import org.apache.commons.lang3.StringEscapeUtils;

/**
 *
 * @author badiehm
 */
public class TrainingDataReader
{
    Hashtable<String, Tweet> TweetsHT = new Hashtable<String, Tweet>();
    Hashtable<String, String> TweetsClassHT = new Hashtable<String, String>();
    String PathSource = "";

    public TrainingDataReader(String PathSource)
    {
        this.PathSource=PathSource;
        Read();
    }

    private void Read()
    {
        InputStreamReader isr=null;
        BufferedReader bufferedReader =null;
        try
        {
            ClassLoader classLoader = new Global().getClass().getClassLoader();
            URL url = classLoader.getResource(PathSource);
            if (url == null)
            {
                throw new RuntimeException("Global:readFile: unable to locate resource: " + PathSource);
            }
            isr = new InputStreamReader(new FileInputStream(url.getFile()), "UTF-8");
            bufferedReader = new BufferedReader(isr);

            String sCurrentLine;
            while ((sCurrentLine = bufferedReader.readLine()) != null)
            {
                ProcessLine(sCurrentLine);
            }
            
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                if (bufferedReader != null)
                {
                    bufferedReader.close();
                }
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    private void ProcessLine(String sCurrentLine)
    {
        try
        {
            sCurrentLine=StringEscapeUtils.unescapeHtml3(sCurrentLine);
            String[] Parts = sCurrentLine.split("\t");
            if (Parts.length != 5)
            {
                System.err.println("Malformated line");
                return;
            }
            String JSON=Parts[4].substring(1, Parts[4].length()-1);
            String TweetStr=Parts[3];
            JSON=JSON.replaceAll("\"\"", "\"");
            Tweet tweet = new Tweet("0",TweetStr,"",JSON);
            TweetsHT.put(tweet.id_str(), tweet);
            TweetsClassHT.put(tweet.id_str(), Parts[2].toUpperCase());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args)
    {
        TrainingDataReader TDR=new TrainingDataReader("False_en_positive_brandmeldingen_balanced.txt");
    }
}
