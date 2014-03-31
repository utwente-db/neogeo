/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.tweet_text_classifier;

import cc.mallet.types.Instance;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import nl.utwente.db.neogeo.twitter.Tweet;

/**
 *
 * @author badiehm
 */
public class TweetIterator implements Iterator<Instance>
{
    Hashtable<String, Tweet> TweetsHT = new Hashtable<String, Tweet>();
    Hashtable<String, String> TweetsClassHT = new Hashtable<String, String>();
    Iterator<Map.Entry<String, Tweet>> TweetsHTIterator =null;

    public TweetIterator(Hashtable<String, Tweet> TweetsHT, Hashtable<String, String> TweetsClassHT)
    {
        this.TweetsClassHT=TweetsClassHT;
        this.TweetsHT=TweetsHT;
        TweetsHTIterator = TweetsHT.entrySet().iterator();
    }

    public boolean hasNext()
    {
        return TweetsHTIterator.hasNext();
    }

    public Instance next()
    {
        Map.Entry<String, Tweet>entry= TweetsHTIterator.next();
        String TweetID= entry.getKey();
        Tweet tweet=entry.getValue();
        String TweetStr=tweet.tweet();
        String label=TweetsClassHT.get(TweetID);
        
        Instance instance=new Instance(TweetStr, label, TweetID, TweetID);
        return instance;
    }

    public void remove()
    {
        throw new IllegalStateException ("This Iterator<Instance> does not support remove().");
    }
    
}