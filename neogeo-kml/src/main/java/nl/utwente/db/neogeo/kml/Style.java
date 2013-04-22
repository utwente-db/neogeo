package nl.utwente.db.neogeo.kml;

public class Style {
    private String id;
    private LineStyle LineStyle = new LineStyle();
    private PolyStyle PolyStyle = new PolyStyle();
	
    public String getId() {
		return id;
	}
	
    public void setId(String id) {
		this.id = id;
	}
	
    public LineStyle getLineStyle() {
		return LineStyle;
	}
	
    public void setLineStyle(LineStyle lineStyle) {
		LineStyle = lineStyle;
	}
	
    public PolyStyle getPolyStyle() {
		return PolyStyle;
	}
	
    public void setPolyStyle(PolyStyle polyStyle) {
		PolyStyle = polyStyle;
	}
}
