package nl.utwente.db.neogeo.kml;

public class GroundOverlay {
	private String name;
	private String description;
	private Icon Icon;
	private LatLonBox LatLonBox;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Icon getIcon() {
		return Icon;
	}

	public void setIcon(Icon icon) {
		Icon = icon;
	}

	public LatLonBox getLatLonBox() {
		return LatLonBox;
	}

	public void setLatLonBox(LatLonBox latLonBox) {
		LatLonBox = latLonBox;
	}
}
