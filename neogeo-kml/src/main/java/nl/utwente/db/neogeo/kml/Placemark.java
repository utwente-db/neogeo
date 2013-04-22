package nl.utwente.db.neogeo.kml;

public class Placemark {
	private String name;
	private String visability;
	private String styleUrl;
	private Polygon Polygon;
	private LineString LineString;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getVisability() {
		return visability;
	}

	public void setVisability(String visability) {
		this.visability = visability;
	}

	public String getStyleUrl() {
		return styleUrl;
	}

	public void setStyleUrl(String styleUrl) {
		this.styleUrl = styleUrl;
	}

	public Polygon getPolygon() {
		return Polygon;
	}

	public void setPolygon(Polygon polygon) {
		Polygon = polygon;
	}

	public LineString getLineString() {
		return LineString;
	}

	public void setLineString(LineString lineString) {
		LineString = lineString;
	}
}