/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.tweet_text_classifier;

import cc.mallet.classify.*;
import cc.mallet.pipe.*;
import cc.mallet.pipe.iterator.*;
import cc.mallet.types.*;
import java.io.File;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import nl.utwente.db.ZehminCRF.utils.Global;
import nl.utwente.db.neogeo.twitter.Tweet;

/**
 *
 * @author badiehm
 */
public class TweetClassifier
{

    Pipe instancePipe = null;
    Classifier classifier = null;
    TrainingDataReader TDR = null;

    public TweetClassifier(String FileName)
    {
        ClassLoader classLoader = new Global().getClass().getClassLoader();
        URL url = classLoader.getResource("nlstopwords.txt");
        
        instancePipe = new SerialPipes(new Pipe[]
                {
                    new Target2Label(), // Target String -> class label
                    new Input2CharSequence(), // Data File -> String containing contents
                   // new CharSubsequence(CharSubsequence.SKIP_HEADER), // Remove UseNet or email header
                    new CharSequence2TokenSequence(), // Data String -> TokenSequence
                    new TokenSequenceLowercase(), // TokenSequence words lowercased
                    new TokenSequenceRemoveStopwords(new File(url.getFile()),"UTF-8",false,false,false),// Remove stopwords from sequence
                    new TokenSequence2FeatureSequence(),// Replace each Token with a feature index
                    new FeatureSequence2FeatureVector(),// Collapse word order into a "feature vector"
                    new PrintInputAndTarget(),
                });
        TDR = new TrainingDataReader(FileName);
    }

    public void Train()
    {
        InstanceList ilist = new InstanceList(instancePipe);

        ilist.addThruPipe(new TweetIterator(TDR.TweetsHT, TDR.TweetsClassHT));
        InstanceList[] ilists = ilist.split(new double[]
                {
                    1, 0
                });

        System.out.println("The training set size is " + ilist.size());
        ClassifierTrainer Trainer = new NaiveBayesTrainer();
        classifier = Trainer.train(ilists[0]);

        System.out.println("The training accuracy is " + classifier.getAccuracy(ilists[0]));
        System.out.println("The testing accuracy is " + classifier.getAccuracy(ilists[0]));
    }

    public String Test(String tweetStr)
    {
        if (classifier == null)
        {
            System.out.println("Train function must be called first!");
            return "";
        }
        Classification classification = classifier.classify(tweetStr);
        String result = classification.getLabeling().getBestLabel().toString();
        System.out.println("The result is " + result);
        return result;
    }

    static public void main(String[] args)
    {
        TweetClassifier TC = new TweetClassifier("False_en_positive_brandmeldingen_balanced.txt");
        TC.Train();

//        Iterator<Map.Entry<String, Tweet>> TweetsHTIterator = TC.TDR.TweetsHT.entrySet().iterator();
//        while (TweetsHTIterator.hasNext())
//        {
//            Map.Entry<String, Tweet>entry= TweetsHTIterator.next();
//            String TweetStr = entry.getValue().tweet();
//            TC.Test(TweetStr);
//        }
        
        String TweetStr = "Terwijl de mensen aan het eten waren in rest De Lichtmis raakte hun voertuig plotseling in brand.";
        TC.Test(TweetStr);
    }
}