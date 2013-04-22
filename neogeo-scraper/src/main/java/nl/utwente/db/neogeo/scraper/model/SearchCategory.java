package nl.utwente.db.neogeo.scraper.model;

import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;

public class SearchCategory extends PointOfInterestCategory {

	@Override
	public String toString() {
		return "SearchCategory [id=" + id + ", timestamp=" + timestamp
				+ ", name=" + name + "]";
	}
}
