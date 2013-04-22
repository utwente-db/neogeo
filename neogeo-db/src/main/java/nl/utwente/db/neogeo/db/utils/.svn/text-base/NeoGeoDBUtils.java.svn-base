package nl.utwente.db.neogeo.db.utils;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import nl.utwente.db.neogeo.core.model.ModelObject;
import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;
import nl.utwente.db.neogeo.core.model.Town;
import nl.utwente.db.neogeo.db.hibernate.BaseModelObjectDAO;
import nl.utwente.db.neogeo.db.hibernate.GenericDAO;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

public abstract class NeoGeoDBUtils {
	public static Town getTownByName(String townName) {
		return (Town)NeoGeoDBUtils.getElementByName(Town.class, townName);
	}

	public static PointOfInterestCategory getCategoryByName(String categoryName) {
		return (PointOfInterestCategory)NeoGeoDBUtils.getElementByName(PointOfInterestCategory.class, categoryName);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static ModelObject getElementByName(Class<? extends ModelObject> clazz, String name) {
		BeanWrapper beanWrapper = new BeanWrapperImpl(clazz);
		ModelObject templateObject = (ModelObject)beanWrapper.getWrappedInstance();
		
		templateObject.setName(name);
		
		GenericDAO<ModelObject> dao = new BaseModelObjectDAO(clazz);
		
		// TODO improve this, instead of the empty set, use inspection to detect primitive data types
		List<ModelObject> results = dao.findByExample(templateObject, (Set)Collections.emptySet());
		
		if (!results.isEmpty()) {
			return results.get(0);
		}
		
		ModelObject result = (ModelObject)new BeanWrapperImpl(clazz).getWrappedInstance();
		
		result.setName(name);
		dao.makePersistent(result);
		
		HibernateUtils.commit();
		
		return result;
	}
}
