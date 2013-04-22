package nl.utwente.db.neogeo.scraper.workflow.tasks.geo;

import nl.utwente.db.neogeo.db.utils.NeoGeoDBUtils;

public class GeoTestSearchSpaceTask extends GeoSearchSpaceTask {
	public GeoTestSearchSpaceTask() {
		addCategory(NeoGeoDBUtils.getCategoryByName("restaurant"));
		addCategory(NeoGeoDBUtils.getCategoryByName("drogisterij"));

		addTown(NeoGeoDBUtils.getTownByName("enschede"));
	}
}
