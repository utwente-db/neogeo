package nl.utwente.db.neogeo.kml;

import java.util.ArrayList;
import java.util.List;

public class Document {

	private String name;
	private String address;
	private String description;
	private String open;
	private List<Style> styles = new ArrayList<Style>();
	private List<Placemark> placemarks = new ArrayList<Placemark>();
	private Folder Folder = new Folder();

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getOpen() {
		return open;
	}

	public void setOpen(String open) {
		this.open = open;
	}

	public List<Style> getStyles() {
		return styles;
	}

	public void setStyles(List<Style> styles) {
		this.styles = styles;
	}

	public List<Placemark> getPlacemarks() {
		return placemarks;
	}

	public void setPlacemarks(List<Placemark> placemarks) {
		this.placemarks = placemarks;
	}

	public Folder getFolder() {
		return Folder;
	}

	public void setFolder(Folder folder) {
		Folder = folder;
	}

}
