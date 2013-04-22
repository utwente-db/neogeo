package nl.utwente.db.neogeo.db.daos;

import java.util.ArrayList;
import java.util.List;

import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;
import nl.utwente.db.neogeo.core.model.Town;
import nl.utwente.db.neogeo.db.hibernate.BaseModelObjectDAO;
import nl.utwente.db.neogeo.db.utils.HibernateUtils;

public class PointOfInterestDAO extends BaseModelObjectDAO<PointOfInterest> {
	@Override
	public List<PointOfInterest> findByExample(PointOfInterest exampleInstance) {
		List<String> excludeProperties = new ArrayList<String>();

		if (exampleInstance.getLatitude() == 0) {
			excludeProperties.add("latitude");
		}

		if (exampleInstance.getLongitude() == 0) {
			excludeProperties.add("longitude");
		}
		
		if (exampleInstance.getX() == 0) {
			excludeProperties.add("x");
		}

		if (exampleInstance.getY() == 0) {
			excludeProperties.add("y");
		}
		
		return findByExample(exampleInstance, excludeProperties);
	}
	
	@Override
	public PointOfInterest makePersistent(PointOfInterest poi) {
		// TODO this must be possible otherwise, but Hibernate keeps throwing TransientObjectExceptions
		// Maybe this has been solved already by setting the FlushMode to MANUAL on the session
		poi.setTown((Town)HibernateUtils.getSession().load(Town.class, poi.getTown().getId()));
		
		poi.setCategory((PointOfInterestCategory)HibernateUtils.getSession().load(PointOfInterestCategory.class, poi.getCategory().getId()));
		
		return super.makePersistent(poi);
	}
}