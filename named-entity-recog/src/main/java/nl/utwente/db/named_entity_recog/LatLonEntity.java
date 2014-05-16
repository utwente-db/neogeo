/* To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

/**
 *
 * @author badiehm + flokstra
 */



public abstract class LatLonEntity extends ResolvedEntity {

    protected double latitude;
    protected double longitude;
    
    public LatLonEntity(NamedEntity entity, char kind, double latitude, double longitude) {
    	super(entity,kind);
    	// entity.addResolved(this);
        this.latitude = latitude;
        this.longitude = longitude;
    }
    
    public double getLatitude() {
    	return this.latitude;
    }
    
    public double getLongitude() {
    	return this.longitude;
    }

}
