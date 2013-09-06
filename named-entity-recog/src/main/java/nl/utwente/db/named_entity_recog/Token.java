/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

/**
 *
 * @author badiehm
 */
public class Token
{
    private String word;
    private int offset;
    
    public Token(String word, int offset)
    {
        this.word = word;
        this.offset = offset;
    }

    public String getWord()
    {
        return word;
    }

    public int getOffset()
    {
        return offset;
    }

    
    public void setWord(String word)
    {
        this.word = word;
    }

    public void setOffset(int offset)
    {
        this.offset = offset;
    }
    
}
