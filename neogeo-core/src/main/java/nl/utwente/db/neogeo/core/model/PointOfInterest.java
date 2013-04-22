package nl.utwente.db.neogeo.core.model;

public class PointOfInterest extends Address {
	protected String url;
	protected String imageUrl;
	protected PointOfInterestCategory category;

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getImageUrl() {
		return imageUrl;
	}

	public void setImageUrl(String imageUrl) {
		this.imageUrl = imageUrl;
	}

	public PointOfInterestCategory getCategory() {
		return category;
	}

	public void setCategory(PointOfInterestCategory category) {
		this.category = category;
	}

	@Override
	public String toString() {
		return "PointOfInterest [url=" + url + ", imageUrl=" + imageUrl
				+ ", category=" + category + ", streetName=" + streetName
				+ ", houseNumber=" + houseNumber + ", postalCode=" + postalCode
				+ ", town=" + town + ", phoneNumber=" + phoneNumber
				+ ", latitude=" + latitude + ", longitude=" + longitude
				+ ", sourceUrl=" + sourceUrl + ", id=" + id + ", timestamp="
				+ timestamp + ", name=" + name + "]";
	}
}
