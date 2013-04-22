package nl.utwente.db.neogeo.kml;

public class Polygon {
    private String extrude;
    private String altitudeMode;
    private OuterBoundaryIs outerBoundaryIs = new OuterBoundaryIs();
    
	public String getExtrude() {
		return extrude;
	}
	
	public void setExtrude(String extrude) {
		this.extrude = extrude;
	}
	
	public String getAltitudeMode() {
		return altitudeMode;
	}
	
	public void setAltitudeMode(String altitudeMode) {
		this.altitudeMode = altitudeMode;
	}
	
	public OuterBoundaryIs getOuterBoundaryIs() {
		return outerBoundaryIs;
	}
	
	public void setOuterBoundaryIs(OuterBoundaryIs outerBoundaryIs) {
		this.outerBoundaryIs = outerBoundaryIs;
	}
}
