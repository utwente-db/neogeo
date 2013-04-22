package nl.utwente.db.neogeo.kml;

import java.util.ArrayList;
import java.util.List;

public class Folder {
	private List<Placemark> placemarks = new ArrayList<Placemark>();
	private List<GroundOverlay> groundoverlays = new ArrayList<GroundOverlay>();
	private String name;
	private String description;

	public List<Placemark> getPlacemarks() {
		return placemarks;
	}

	public void setPlacemarks(List<Placemark> placemarks) {
		this.placemarks = placemarks;
	}

	public List<GroundOverlay> getGroundoverlays() {
		return groundoverlays;
	}

	public void setGroundoverlays(List<GroundOverlay> groundoverlays) {
		this.groundoverlays = groundoverlays;
	}

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
}
