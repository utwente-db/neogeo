/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import nl.utwente.db.ZehminCRF.corpus.Corpus;
import nl.utwente.db.ZehminCRF.main.Decoder;
import nl.utwente.db.ZehminCRF.sp.CRModel_sp1;


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
        //String TweetStr = "Onderweg naar Enschede voor hopelijk een mooi feestje vanavond. #batavieren";
        String TweetStr = "Campuspop:Alle Batavieren kunnen morgen voor 16 Euro kaarten kopen voor Campuspop met Anouk, Candy Dulfer, Ben Saundersenz.#batavierenrace";

        //PrepareTrainingFile();
        PrepareTestFile(TweetStr);

        Corpus train_corpus = new Corpus("crfTrain.txt");
        System.out.println("#Training Sentences: " + train_corpus.getNumSentences());

        Corpus test_corpus = new Corpus("crfTest.txt");
        System.out.println("#Test Sentences: " + test_corpus.getNumSentences());

        System.out.println("HERE-1");
        CRModel_sp1 model = new CRModel_sp1(train_corpus, "CRF.model", false);
        System.out.println("HERE-2");
        List<NamedEntity> NEs= new Decoder(train_corpus, test_corpus, model).decodeTopK(TweetStr,1);
        System.out.println("HERE-3");
        
        for(int i=0;i<NEs.size();i++)
        {
            System.out.println(NEs.get(i).getMention());
        }
    }

    private static void PrepareTrainingFile()
    {
        try
        {
            String PathSource = "D:/PhD/Data/Dutch NER/ned.train";
            String PathTarget = "crfTrain.txt";

            FileOutputStream fos = new FileOutputStream(PathTarget);
            OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");

            InputStreamReader isr = new InputStreamReader(new FileInputStream(PathSource), "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(isr);
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
                            tag = "NE";
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

    private static void PrepareTestFile(String TweetStr)
    {
        try
        {
            String PathTarget = "crfTest.txt";

            FileOutputStream fos = new FileOutputStream(PathTarget);
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
                out.write(Token + "\t" + feature +"\t" + "O");
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
}
