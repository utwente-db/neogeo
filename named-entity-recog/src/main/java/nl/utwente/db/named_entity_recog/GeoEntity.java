/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.named_entity_recog;

/**
 *
 * @author badiehm + flokstra
 */

class GeoEntity extends ResolvedEntity {

    private double latitude;
    private double longitude;
    private String country;
    private String alternatenames;
    private int	population;
    private int elevation;
    private String fclass; // feature class

    public GeoEntity(NamedEntity entity, double latitude, double longitude,String country, String alternatenames, int population, int elevation, String fclass)
    {
    	super(entity,ResolvedEntity.GEO_ENTITY);
    	entity.addResolved(this);
        this.latitude = latitude;
        this.longitude = longitude;
        this.country = country;
        this.alternatenames = alternatenames;
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
