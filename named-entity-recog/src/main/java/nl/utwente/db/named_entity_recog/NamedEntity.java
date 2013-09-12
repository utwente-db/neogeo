/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

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

    public NamedEntity(String mention,String tag, int offset, double score)
    {
        this.mention = mention;
        this.tag=tag;
        this.offset = offset;
        this.score = score;
    }

    public String getMention()
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
    
}
