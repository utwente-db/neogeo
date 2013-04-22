package nl.utwente.db.neogeo.graaff.pse;

import java.util.ArrayList;
import java.util.List;

import nl.utwente.db.neogeo.kml.Document;
import nl.utwente.db.neogeo.kml.Kml;
import nl.utwente.db.neogeo.kml.LinearRing;
import nl.utwente.db.neogeo.kml.OuterBoundaryIs;
import nl.utwente.db.neogeo.kml.Placemark;
import nl.utwente.db.neogeo.kml.Polygon;
import nl.utwente.db.neogeo.kml.utils.KmlUtils;
import nl.utwente.db.neogeo.utils.FileUtils;

public class KMLGenerator {
	public static void main(String[] args) {
		KMLGenerator kmlGenerator = new KMLGenerator();
		Kml kml = kmlGenerator.generateCircleKML();
		
		String kmlText = KmlUtils.toKML(kml);
		FileUtils.writeFile("circles.kml", kmlText);
	}
	
	public Kml generateCircleKML() {
		LinearRing linearRing1 = new LinearRing();
		linearRing1.setCoordinates("52.237924,6.861124,0.52.237924,6.861124,0.52.237924,6.861124,0.52.237924,6.861124,0.‎");
		
		OuterBoundaryIs outerBoundaryIs1 = new OuterBoundaryIs();
		outerBoundaryIs1.setLinearRing(linearRing1);
		
		Polygon polygon1 = new Polygon();
		polygon1.setOuterBoundaryIs(outerBoundaryIs1);
		
		Placemark placemark1 = new Placemark();
		placemark1.setName("Victor's office");
		placemark1.setPolygon(polygon1);
		
		List<Placemark> placemarks = new ArrayList<Placemark>();
		placemarks.add(placemark1);
		
		Document document = new Document();
		document.setName("Victor's test");
		document.setDescription("Victor's KML Test");
		document.setPlacemarks(placemarks);
		
		Kml kml = new Kml();
		kml.setDocument(document);
		
		return kml;
	}
}
