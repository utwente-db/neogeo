package nl.utwente.db.neogeo.graaff.pse;

import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;
import nl.utwente.db.neogeo.db.daos.PointOfInterestDAO;
import nl.utwente.db.neogeo.db.utils.NeoGeoDBUtils;

public class Test {
	public static void main(String[] args) {
		PointOfInterestCategory category = NeoGeoDBUtils.getCategoryByName("Restaurants");
		PointOfInterestDAO poiDAO = new PointOfInterestDAO();
		
		PointOfInterest poiTemplate = new PointOfInterest();
		poiTemplate.setCategory(category);
		
		System.out.println(poiDAO.findByExample(poiTemplate).size());
	}
}
