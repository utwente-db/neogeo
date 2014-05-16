/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

/**
 *
 * @author badiehm + flokstra
 */

public class GeoNameEntity extends LatLonEntity {

    private String country;
    private String alternatenames;
    private int	population;
    private int elevation;
    private String fclass; // feature class

    public static final int minLength = 3;
    
    public GeoNameEntity(NamedEntity entity, double latitude, double longitude,String country, String alternatenames, int population, int elevation, String fclass)
    {
    	super(entity,ResolvedEntity.GEO_ENTITY,latitude,longitude);
    	// entity.addResolved(this);
        this.country = country;
        if ( alternatenames != null )
        	this.alternatenames = alternatenames;
        else
        	this.alternatenames = "";
        this.population = population;
        this.elevation = elevation;
        this.fclass = fclass;
    }
    
    public double getLatitude() {
    	return this.latitude;
    }
    
    public double getLongitude() {
    	return this.longitude;
    }
    
    public String getCountry() {
    	return  country;
    }
    
    public String getAlternatenames() {
    	return alternatenames;
    }
    
    public int getPopulation() {
    	return population;
    }
    
    public int getElevation() {
    	return elevation;
    }
    
    public String getFeatureClass() {
    	return fclass;
    }
    
    public String toString() {
    	return "GeoEntity"+getEntity().toString()+"{country="+getCountry()+", latitude="+getLatitude()+", longitude="+getLongitude()+
    			", alternate="+getAlternatenames() + ", population="+getPopulation() + ", elevation="+getElevation() + ", fclass="+getFeatureClass() +
    		"}";
    }
}
