package nl.utwente.db.neogeo.scraper.utils;

import java.util.List;

import nl.utwente.db.neogeo.core.model.ModelObject;
import nl.utwente.db.neogeo.db.utils.NeoGeoDBUtils;
import nl.utwente.db.neogeo.utils.SpringUtils;
import nl.utwente.db.neogeo.utils.StringUtils;
import nl.utwente.db.neogeo.utils.WebUtils;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

import com.gargoylesoftware.htmlunit.html.HtmlElement;

public class BaseModelMapper implements ModelMapper {
	public void fillObject(ModelObject modelObject, HtmlElement htmlElement) {
		// TODO improve this using knowledge of the data types, for example phone numbers etc.
		BeanWrapper beanWrapper = new BeanWrapperImpl(modelObject);
		List<String> variableNames = SpringUtils.getWritableVariableNamesForBeanWrapper(beanWrapper);
		
		if (ScraperUtils.isTableOrTableBody(htmlElement)) {
			fillObjectFromTable(modelObject, variableNames, htmlElement);
		} else if (ScraperUtils.isDefinitionList(htmlElement)) {
			fillObjectFromDefinitionList(modelObject, variableNames, htmlElement);
		} else if (ScraperUtils.isTwoColumnDivGrid(htmlElement)) {
			fillObjectFromTwoColumnDivGrid(modelObject, variableNames, htmlElement);
		} else {
			fillObjectByTagAttributes(modelObject, variableNames, htmlElement);
		}
	}

	protected void fillObjectFromTable(ModelObject modelObject, List<String> variableNames, HtmlElement htmlElement) {
		// TODO implement this
	}
	
	protected void fillObjectFromDefinitionList(ModelObject modelObject, List<String> variableNames, HtmlElement htmlElement) {
		// TODO implement this
	}
	
	protected void fillObjectFromTwoColumnDivGrid(ModelObject modelObject, List<String> variableNames, HtmlElement htmlElement) {
		// TODO implement this
	}
	
	@SuppressWarnings("unchecked")
	protected void fillObjectByTagAttributes(ModelObject modelObject, List<String> variableNames, HtmlElement htmlElement) {
		for (String variableName : variableNames) {
			String xPath = ".//*[contains(@class, '" + variableName + "')]";
			List<HtmlElement> possibleElements = (List<HtmlElement>)htmlElement.getByXPath(xPath);
			
			if (possibleElements.isEmpty()) {
				continue;
			}
			
			HtmlElement element = possibleElements.get(0);
			Object propertyValue = extractContent(element);
			
			if (SpringUtils.isModelObject(modelObject, variableName)) {
				propertyValue = NeoGeoDBUtils.getElementByName((Class<? extends ModelObject>)SpringUtils.getPropertyType(modelObject, variableName), (String)propertyValue);
			}
			
			SpringUtils.setProperty(modelObject, variableName, propertyValue);
		}
	}

	@SuppressWarnings("unchecked")
	private String extractContent(HtmlElement htmlElement) {
		// 1) Try to get the text content
		String result = htmlElement.asText();
		
		// 2) If nothing found, try to get the titles
		if (StringUtils.isEmpty(result)) {
			String xPath = ".//*[@title]";
			List<HtmlElement> titledElements = (List<HtmlElement>)htmlElement.getByXPath(xPath);
			
			if (!titledElements.isEmpty()) {
				result = WebUtils.getAttributeValue(titledElements.get(0), "title");
			}
		}
		
		// 3) If nothing found, try to get the alt tags
		if (StringUtils.isEmpty(result)) {
			String xPath = ".//*[@alt]";
			List<HtmlElement> elementsWithAlternativeText = (List<HtmlElement>)htmlElement.getByXPath(xPath);
			
			if (!elementsWithAlternativeText.isEmpty()) {
				result = WebUtils.getAttributeValue(elementsWithAlternativeText.get(0), "alt");
			}
		}
		
		return result;
		
	}
}
