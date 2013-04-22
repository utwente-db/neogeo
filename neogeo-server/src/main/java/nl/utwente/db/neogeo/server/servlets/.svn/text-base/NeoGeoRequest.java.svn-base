package nl.utwente.db.neogeo.server.servlets;

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.server.InvalidRequestException;
import nl.utwente.db.neogeo.server.utils.ServerUtils;
import nl.utwente.db.neogeo.utils.SpringUtils;
import nl.utwente.db.neogeo.utils.StringUtils;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

public abstract class NeoGeoRequest {
	protected List<String> requiredParameters = new ArrayList<String>();
	protected String referrer;
	
	public void initFromServletRequest(HttpServletRequest request) throws InvalidRequestException {
		BeanWrapper beanWrapper = new BeanWrapperImpl(this);
		
		for (PropertyDescriptor descriptor : beanWrapper.getPropertyDescriptors()) {
			String key = descriptor.getName();
			String value = request.getParameter(key);
			
			if (value == null) {
				continue;
			}
			
			if (beanWrapper.isWritableProperty(key)) {
				if (List.class.isAssignableFrom(SpringUtils.getPropertyType(beanWrapper, key))) {
					// Comma separated values
					beanWrapper.setPropertyValue(key, Arrays.asList(value.split(",")));
				} else {
					beanWrapper.setPropertyValue(key, value);
				}
			}
		}
		
		this.referrer = ServerUtils.getReferrer(request);
		
		this.validate();
	}
	
	public void validate() throws InvalidRequestException {
		BeanWrapper beanWrapper = new BeanWrapperImpl(this);

		for (String requiredParameter : requiredParameters) {
			Object value = beanWrapper.getPropertyValue(requiredParameter);
			
			if (value == null || (value instanceof String && StringUtils.isEmpty((String)value))) {
				throw new NeoGeoException(this.getClass().getSimpleName() + " requires the parameter \"" + requiredParameter + "\"");
			}
		}
	}
	
	public List<String> getRequiredParameters() {
		return requiredParameters;
	}

	
	public String getReferrer() {
		return referrer;
	}
	

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}
}