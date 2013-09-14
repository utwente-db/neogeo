/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

import java.util.Vector;

/**
 *
 * @author badiehm
 */
public class NamedEntity
{
    private String mention;
    private String tag;
    private int offset;
    private double score;

    private Vector<ResolvedEntity> resolved;

    public NamedEntity(String mention,String tag, int offset, double score)
    {
        this.mention = mention;
        this.tag=tag;
        this.offset = offset;
        this.score = score;
	//
    	this.resolved = new Vector<ResolvedEntity>();
    }

    public String getMention()
    {
        return mention;
    }
    
    public String getName()
    {
        return mention;
    }
    
    public String getTag()
    {
        return tag;
    }

    public int getOffset()
    {
        return offset;
    }

    public double getScore()
    {
        return score;
    }

    public void setMention(String mention)
    {
        this.mention = mention;
    }

    public void setTag(String tag)
    {
        this.tag = tag;
    }

    public void setOffset(int offset)
    {
        this.offset = offset;
    }

    public void setScore(double score)
    {
        this.score = score;
    }
    
    boolean isResolved() {
    	return resolved.size() > 0;
    }
    
    public Vector<ResolvedEntity> getResolved() {
    	return resolved;
    }
    
    public void addResolved(ResolvedEntity re) {
    	resolved.add(re);
    }
    
    public String toString() {
    	return "[name="+getName()+", pos="+getOffset()+", tag="+getTag()+", score="+getScore()+"]";
    }
    
}
