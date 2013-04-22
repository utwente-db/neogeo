package nl.utwente.db.neogeo.core.model;

public class Address extends BaseModelObject {
	protected String streetName;
	protected String houseNumber;
	protected String postalCode;
	protected Town town;
	protected String phoneNumber;
	protected double latitude;
	protected double longitude;
	protected String sourceUrl;
	protected double x;
	protected double y;


	public String getStreetName() {
		return streetName;
	}

	public void setStreetName(String streetName) {
		this.streetName = streetName;
	}

	public String getHouseNumber() {
		return houseNumber;
	}

	public void setHouseNumber(String houseNumber) {
		this.houseNumber = houseNumber;
	}

	public String getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}

	public Town getTown() {
		return town;
	}

	public void setTown(Town town) {
		this.town = town;
	}

	public String getPhoneNumber() {
		return phoneNumber;
	}

	public void setPhoneNumber(String phoneNumber) {
		if (phoneNumber != null) {
			this.phoneNumber = phoneNumber.replaceAll( "[^\\d]", "" );
		}
	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public String getSourceUrl() {
		return sourceUrl;
	}

	public void setSourceUrl(String sourceUrl) {
		this.sourceUrl = sourceUrl;
	}
	
	public void setX(double x) {
		this.x = x;
	}

	public double getX() {
		return x;
	}

	public void setY(double y) {
		this.y = y;
	}

	public double getY() {
		return y;
	}

	@Override
	public String toString() {
		return "Address [streetName=" + streetName + ", houseNumber="
				+ houseNumber + ", postalCode=" + postalCode + ", town=" + town
				+ ", phoneNumber=" + phoneNumber + ", latitude=" + latitude
				+ ", longitude=" + longitude + ", sourceUrl=" + sourceUrl
				+ ", x=" + x + ", y=" + y + ", id=" + id + ", timestamp="
				+ timestamp + ", name=" + name + "]";
	}
}
