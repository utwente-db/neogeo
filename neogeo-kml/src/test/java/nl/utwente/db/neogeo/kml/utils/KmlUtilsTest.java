package nl.utwente.db.neogeo.kml.utils;

import java.util.ArrayList;
import java.util.List;

import nl.utwente.db.neogeo.kml.Folder;
import nl.utwente.db.neogeo.kml.GroundOverlay;
import nl.utwente.db.neogeo.kml.Icon;
import nl.utwente.db.neogeo.kml.Kml;
import nl.utwente.db.neogeo.kml.LatLonBox;

import org.junit.Test;
import org.springframework.util.Assert;

public class KmlUtilsTest {

	@Test
	public void fromKML() {
		String kml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
 "				<kml xmlns=\"http://www.opengis.net/kml/2.2\">" +
 "				  <Folder>" +
 "				    <name>Ground Overlays</name>" +
 "				    <description>Examples of ground overlays</description>" +
 "				    <GroundOverlay>" +
 "				      <name>Large-scale overlay on terrain</name>" +
 "				      <description>Overlay shows Mount Etna erupting" +
 "				          on July 13th, 2001.</description>" +
 "				      <Icon>" +
 "				        <href>http://developers.google.com/kml/documentation/images/etna.jpg</href>" +
 "				      </Icon>" +
 "				      <LatLonBox>" +
 "				        <north>37.91904192681665</north>" +
 "				        <south>37.46543388598137</south>" +
 "				        <east>15.35832653742206</east>" +
 "				        <west>14.60128369746704</west>" +
 "				        <rotation>-0.1556640799496235</rotation>" +
 "				      </LatLonBox>" +
 "				    </GroundOverlay>" +
 "				  </Folder>" +
 "				</kml>";
		
		Kml kmlObject = KmlUtils.fromKML(kml);
		
		Assert.notNull(kmlObject);
	}
	
	@Test
	public void toKML() {
		Kml kmlObject = new Kml();
		
		Icon icon = new Icon();
		icon.setHref("http://developers.google.com/kml/documentation/images/etna.jpg");
		
		LatLonBox latLonBox = new LatLonBox();
		latLonBox.setNorth("37.91904192681665");
		latLonBox.setSouth("37.46543388598137");
		latLonBox.setEast("15.35832653742206");
		latLonBox.setWest("14.60128369746704");
		latLonBox.setRotation("-0.1556640799496235");
		
		GroundOverlay groundOverlay = new GroundOverlay();
		groundOverlay.setName("Large-scale overlay on terrain");
		groundOverlay.setDescription("Overlay shows Mount Etna erupting on July 13th, 2001.");
		groundOverlay.setIcon(icon);
		groundOverlay.setLatLonBox(latLonBox);
		
		List<GroundOverlay> groundOverlays = new ArrayList<GroundOverlay>();
		groundOverlays.add(groundOverlay);
		
		Folder folder = new Folder();
		folder.setName("Ground Overlays");
		folder.setDescription("Examples of ground overlays");
		folder.setGroundoverlays(groundOverlays);
		
		kmlObject.setFolder(folder);
		
		System.out.println(KmlUtils.toKML(kmlObject));
		Assert.notNull(KmlUtils.toKML(kmlObject));
	}
}
