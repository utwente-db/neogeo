package nl.utwente.db.neogeo.kml.utils;

import nl.utwente.db.neogeo.kml.Document;
import nl.utwente.db.neogeo.kml.Folder;
import nl.utwente.db.neogeo.kml.GroundOverlay;
import nl.utwente.db.neogeo.kml.Kml;
import nl.utwente.db.neogeo.kml.LinearRing;
import nl.utwente.db.neogeo.kml.OuterBoundaryIs;
import nl.utwente.db.neogeo.kml.Placemark;
import nl.utwente.db.neogeo.kml.Polygon;
import nl.utwente.db.neogeo.kml.Style;

import com.thoughtworks.xstream.XStream;

public abstract class KmlUtils {
	public static Kml fromKML(String kml) {
		XStream xStream = getXStream();
		
		return (Kml)xStream.fromXML(kml);
	}
	
	public static String toKML(Kml kml) {
		XStream xStream = getXStream();
		
		return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + xStream.toXML(kml);
	}

	public static XStream getXStream() {
		XStream xStream = new XStream();
        
        xStream.alias("kml", Kml.class);
        xStream.useAttributeFor(Kml.class, "xmlns");
        xStream.useAttributeFor(Kml.class, "hint");
                
        xStream.alias("Document", Document.class);
        xStream.alias("Style", Style.class);
        xStream.useAttributeFor(Style.class, "id");
        
        xStream.alias("Folder", Folder.class);
        xStream.alias("Placemark", Placemark.class);
        xStream.alias("GroundOverlay", GroundOverlay.class);
        xStream.alias("LinearRing", LinearRing.class);
        xStream.alias("Polygon", Polygon.class);
        xStream.alias("outerBoundaryIs", OuterBoundaryIs.class);
        
        xStream.addImplicitCollection(Document.class, "styles", Style.class);
        xStream.addImplicitCollection(Folder.class, "placemarks", Placemark.class);
        xStream.addImplicitCollection(Folder.class, "groundoverlays", GroundOverlay.class);
        
        return xStream;
	}
}
