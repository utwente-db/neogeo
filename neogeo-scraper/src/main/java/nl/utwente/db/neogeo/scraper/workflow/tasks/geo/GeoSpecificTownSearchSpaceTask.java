package nl.utwente.db.neogeo.scraper.workflow.tasks.geo;

import java.util.List;

import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;
import nl.utwente.db.neogeo.db.daos.PointOfInterestCategoryDAO;
import nl.utwente.db.neogeo.db.utils.NeoGeoDBUtils;
import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.utils.StringUtils;

public class GeoSpecificTownSearchSpaceTask extends GeoSearchSpaceTask {
	public GeoSpecificTownSearchSpaceTask(String townName) {
		this.addTown(NeoGeoDBUtils.getTownByName(townName));
		this.addCategoryNames();
	}
	
	public GeoSpecificTownSearchSpaceTask(String townName, String firstCategoryName) {
		if (townName == null) {
			throw new ScraperException("Unable to start " + GeoSpecificTownSearchSpaceTask.class.getSimpleName() + " without a townName.");
		}
		
		this.addTown(NeoGeoDBUtils.getTownByName(townName));
		this.addCategoryNames(firstCategoryName);
	}

	private void addCategoryNames() {
		this.addCategoryNames(null);
	}

	private void addCategoryNames(String firstCategoryName) {
		PointOfInterestCategoryDAO poiCategoryDao = new PointOfInterestCategoryDAO();
		List<PointOfInterestCategory> poiCategories = poiCategoryDao.findAll();
		
		boolean started = StringUtils.isEmpty(firstCategoryName);

		for (PointOfInterestCategory poiCategory : poiCategories) {
			if (poiCategory.getName().equals(firstCategoryName)) {
				started = true;
			}
			
			this.addCategory(poiCategory);
			
			if (!started) {
				categoryIndex++;
			}
		}
	}
}
