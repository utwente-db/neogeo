package nl.utwente.db.neogeo.utils;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.core.model.ModelObject;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

public class SpringUtils {
	public static void fillFromMap(Object object, Map<String, ? extends Object> keyValueMap) {
		BeanWrapper beanWrapper = new BeanWrapperImpl(object);
		
		for (PropertyDescriptor descriptor : beanWrapper.getPropertyDescriptors()) {
			String propertyName = descriptor.getName();
			Object propertyValue = keyValueMap.get(propertyName);
			
			setProperty(beanWrapper, propertyName, propertyValue);
		}
	}

	public static boolean setProperty(Object object, String propertyName, Object propertyValue) {
		return setProperty(new BeanWrapperImpl(object), propertyName, propertyValue);
	}
	
	public static boolean setProperty(BeanWrapper beanWrapper, String propertyName, Object propertyValue) {
		boolean success = false;
		
		if (beanWrapper.isWritableProperty(propertyName)) {
			if (getPropertyType(beanWrapper, propertyName).equals(double.class)
					&& propertyValue.getClass().equals(String.class)) {
				if (StringUtils.isEmpty((String)propertyValue)) {
					propertyValue = 0;
				} else {
					propertyValue = Double.parseDouble((String)propertyValue);
				}
			}
			
			beanWrapper.setPropertyValue(propertyName, propertyValue);
			success = true;
		}
		
		return success;
	}

	public static void setProperty(Object object, Class<? extends Object> propertyClass, Object propertyValue) {
		BeanWrapper beanWrapper = new BeanWrapperImpl(object);
		String propertyName = getPropertyName(beanWrapper, propertyClass);
		
		beanWrapper.setPropertyValue(propertyName, propertyValue);
	}

	public static Object getProperty(Object object, Class<? extends Object> propertyClass) {
		BeanWrapper beanWrapper = new BeanWrapperImpl(object);
		String propertyName = getPropertyName(beanWrapper, propertyClass);
		
		return beanWrapper.getPropertyValue(propertyName);
	}

	public static Object getProperty(Object object, String propertyName) {
		BeanWrapper beanWrapper = new BeanWrapperImpl(object);
		
		if (beanWrapper.isReadableProperty(propertyName)) {
			return beanWrapper.getPropertyValue(propertyName);
		}
		
		return null;
	}

	public static String getPropertyName(Object object, Class<? extends Object> propertyClass) {
		return getPropertyName(new BeanWrapperImpl(object), propertyClass);
	}

	public static String getPropertyName(BeanWrapper beanWrapper, Class<? extends Object> propertyClass) {
		PropertyDescriptor descriptor = null;

		for (PropertyDescriptor potentialDescriptor : beanWrapper.getPropertyDescriptors()) {
			if (propertyClass.isInstance(beanWrapper.getPropertyValue(potentialDescriptor.getName()))) {
				if (descriptor == null) {
					descriptor = potentialDescriptor;
				} else {
					throw new NeoGeoException("Unable to determine which property to set for class " + propertyClass + " in bean " + beanWrapper.getWrappedInstance() + ". Use setProperty(BeanWrapper beanWrapper, String propertyName, Object propertyValue) instead.");
				}
			}
		}
		
		if (descriptor == null) {
			throw new NeoGeoException("Unable to find property with class " + propertyClass + " for object " + beanWrapper.getWrappedInstance());
		}
		
		return descriptor.getName();
	}

	public static String getPropertyNameStartingWith(Object object, String propertyPrefix) {
		BeanWrapper beanWrapper = new BeanWrapperImpl(object);
		PropertyDescriptor descriptor = null;

		for (PropertyDescriptor potentialDescriptor : beanWrapper.getPropertyDescriptors()) {
			if (potentialDescriptor.getName().startsWith(propertyPrefix)) {
				if (descriptor == null) {
					descriptor = potentialDescriptor;
				} else {
					throw new NeoGeoException("Unable to determine which property to choose for prefix " + propertyPrefix + " in bean " + beanWrapper.getWrappedInstance() + ". Please be more specific.");
				}
			}
		}
		
		if (descriptor == null) {
			throw new NeoGeoException("Unable to find property with prefix or exact name " + propertyPrefix + " for object " + beanWrapper.getWrappedInstance());
		}
		
		return descriptor.getName();
	}

	public static List<String> getWritableVariableNames(Object object) {
		return getWritableVariableNamesForBeanWrapper(new BeanWrapperImpl(object));
	}

	public static List<String> getWritableVariableNamesForBeanWrapper(BeanWrapper beanWrapper) {
		List<String> result = new ArrayList<String>();
		
		for (PropertyDescriptor propertyDescriptor : beanWrapper.getPropertyDescriptors()) {
			String propertyName = propertyDescriptor.getName();
			
			if (beanWrapper.isWritableProperty(propertyName)) {
				result.add(propertyName);
			}
		}
		
		return result;
	}

	public static List<String> getReadableVariableNames(Object object) {
		return getReadableVariableNamesForBeanWrapper(new BeanWrapperImpl(object));
	}
	
	public static List<String> getReadableVariableNamesForBeanWrapper(BeanWrapper beanWrapper) {
		List<String> result = new ArrayList<String>();
		
		for (PropertyDescriptor propertyDescriptor : beanWrapper.getPropertyDescriptors()) {
			String propertyName = propertyDescriptor.getName();
			
			if (beanWrapper.isReadableProperty(propertyName)) {
				result.add(propertyName);
			}
		}
		
		return result;
	}
	
	public static boolean isModelObject(ModelObject modelObject, String variableName) {
		return ModelObject.class.isAssignableFrom(getPropertyType(modelObject, variableName));
	}
	
	public static Class<? extends Object> getPropertyType(ModelObject modelObject, String variableName) {
		return getPropertyType(new BeanWrapperImpl(modelObject), variableName);
	}
	
	public static Class<? extends Object> getPropertyType(BeanWrapper beanWrapper, String variableName) {
		return beanWrapper.getPropertyDescriptor(variableName).getPropertyType();
	}

}
