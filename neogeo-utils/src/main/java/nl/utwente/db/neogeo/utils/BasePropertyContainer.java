package nl.utwente.db.neogeo.utils;

import java.util.HashMap;
import java.util.Map;

import nl.utwente.db.neogeo.core.NeoGeoException;

import org.apache.log4j.Logger;
import org.springframework.util.ClassUtils;

public class BasePropertyContainer implements PropertyContainer {
	protected Logger logger = Logger.getLogger(BasePropertyContainer.class);

	protected Map<String, Object> propertiesByName = new HashMap<String, Object>();
	protected Map<Class<? extends Object>, Object> propertiesByClass = new HashMap<Class<? extends Object>, Object>();

	private Object object;

	public BasePropertyContainer() {}
	
	public BasePropertyContainer(Object object) {
		this.object = object;
	}
	
	public void setProperty(String propertyName, Object propertyValue) {
		boolean isSettableProperty = SpringUtils.setProperty(getObject(), propertyName, propertyValue);

		if (!isSettableProperty) {
			// Store in hashmaps instead
			propertiesByName.put(propertyName, propertyValue);
		
			if (!ClassUtils.isPrimitiveOrWrapper(propertyValue.getClass())
				&& !String.class.equals(propertyValue.getClass())
				&& propertiesByClass.put(propertyValue.getClass(), propertyValue) != null) {
				// Primitives and string pollute the log, while it may be clear to the user that this is not an unambiguous type to rely on
				logger.debug("Overwriting property for class " + propertyValue.getClass() + " for object " + this + ". " +
						"Using getProperty(Class clazz) to retrieve this property later is not recommended, use getProperty(String propertyName) instead.");
			}
		}
	}
	
	public void setProperty(Class<? extends Object> propertyClass, Object propertyValue) {
		String propertyName = null;
		
		try {
			propertyName = SpringUtils.getPropertyName(getObject(), propertyClass);
		} catch (NeoGeoException e) {
			// Zero or multiple fields for this class
			propertiesByClass.put(propertyValue.getClass(), propertyValue);
			return;
		}
		
		boolean isSettableProperty = SpringUtils.setProperty(this, propertyName, propertyValue);
		
		if (!isSettableProperty) {
			propertiesByName.put(propertyName, propertyValue);
			propertiesByClass.put(propertyClass, propertyValue);
		}
	}
	
	public Object getProperty(String propertyName) {
		Object result = SpringUtils.getProperty(getObject(), propertyName);
		
		if (result == null) {
			result = propertiesByName.get(propertyName);
		}

		return result;
	}

	public String getStringProperty(String propertyName) {
		return (String)getProperty(propertyName);
	}

	public Object getProperty(Class<? extends Object> clazz) {
		Object result = null;
		
		try {
			// 1) Find field on object of this type
			result = SpringUtils.getProperty(getObject(), clazz);
		} catch (NeoGeoException e) {
			// 2) No such field or value found, look in the properties map fast
			result = propertiesByClass.get(clazz);
		}
		
		if (result == null) {
			// 3) No direct match found, find subclass object
			for (Object object : propertiesByClass.values()) {
				if (clazz.isInstance(object)) {
					result = object;
					break;
				}
			}
		}
		
		return result;
	}

	private Object getObject() {
		if (object == null) {
			return this;
		} else {
			return object;
		}
	}
}
