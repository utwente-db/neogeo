package nl.utwente.db.named_entity_recog;

public class StreetEntity extends LatLonEntity {

	public String city;
	public String municipality;
	public String province;
	public String street;
	
	public StreetEntity(NamedEntity entity, String city,String municipality,String province,String street,double latitude,double longitude) {
		super(entity,ResolvedEntity.STREET_ENTITY,latitude,longitude);
		this.city = city;
		this.municipality = municipality;
		this.province = province;
		this.street = street;
	}
	
	public String toString() {
		return "Street<"+street+","+city+","+municipality+","+province+",lat="+latitude+",lon="+longitude+">";
	}
}