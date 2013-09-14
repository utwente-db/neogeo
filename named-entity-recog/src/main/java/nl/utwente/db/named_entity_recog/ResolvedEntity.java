/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

/**
 *
 * @author badiehm & flokstra
 */
public abstract class ResolvedEntity
{
    public static final char GEO_ENTITY = 'g';
    public static final char UNKNOWN_ENTITY = 'u';

    private NamedEntity entity;
    private char kind;

    protected ResolvedEntity(NamedEntity entity, char kind)
    {
        this.entity = entity;
	this.kind = kind;
    }

    public char getKind()
    {
        return this.kind;
    }

    public NamedEntity getEntity()
    {
        return entity;
    }
    
}
