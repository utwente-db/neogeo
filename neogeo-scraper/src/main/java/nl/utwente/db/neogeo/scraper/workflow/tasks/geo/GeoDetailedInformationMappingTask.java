package nl.utwente.db.neogeo.scraper.workflow.tasks.geo;

import java.util.List;

import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;
import nl.utwente.db.neogeo.core.model.Town;
import nl.utwente.db.neogeo.db.utils.NeoGeoDBUtils;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.utils.AddressHelper;
import nl.utwente.db.neogeo.scraper.utils.BaseModelMapper;
import nl.utwente.db.neogeo.scraper.utils.ModelMapper;
import nl.utwente.db.neogeo.scraper.workflow.tasks.AbstractScraperTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.DetailedInformationDetectionTask;
import nl.utwente.db.neogeo.scraper.xpathdetectors.AddressDetector;
import nl.utwente.db.neogeo.utils.StringUtils;
import nl.utwente.db.neogeo.utils.WebUtils;

import org.w3c.dom.Node;

import com.gargoylesoftware.htmlunit.html.DomNode;
import com.gargoylesoftware.htmlunit.html.DomNodeList;
import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlElement;

public class GeoDetailedInformationMappingTask extends AbstractScraperTask<HtmlElement, PointOfInterest> {

	public ScraperMessage<PointOfInterest> next() {
		ScraperMessage<HtmlElement> inputMessage = getNextInputMessage();
		HtmlElement addressComponent = inputMessage.getBody();

		PointOfInterest poi = new PointOfInterest();

		// Initial filling
		ModelMapper baseModelMapper = new BaseModelMapper();
		baseModelMapper.fillObject(poi, addressComponent);
		
		// Improve contents
		AddressDetector addressDetector = new AddressDetector();

		poi.setName(findName(addressComponent));
		poi.setPhoneNumber(findPhoneNumber(addressComponent, addressDetector));

		findAddress(poi, addressComponent, addressDetector);

		poi.setPostalCode(findPostalCode(addressComponent, addressDetector));

		Town town = NeoGeoDBUtils.getTownByName(findTownName(inputMessage, poi.getPostalCode(), addressComponent));
		poi.setTown(town);

		poi.setUrl(findURL(inputMessage, addressComponent));
		poi.setImageUrl(findImageURL(addressComponent));

		poi.setSourceUrl((String)inputMessage.getProperty(DetailedInformationDetectionTask.DETAILED_INFORMATION_SOURCE_URL));
		
		if (poi.getCategory() == null) {
			poi.setCategory((PointOfInterestCategory)inputMessage.getProperty(PointOfInterestCategory.class));
		}

		return createScraperMessage(poi);
	}

	@SuppressWarnings("unchecked")
	protected String findURL(ScraperMessage<HtmlElement> inputMessage, HtmlElement addressComponent) {
		String sourceURL = (String)inputMessage.getProperty(DetailedInformationDetectionTask.DETAILED_INFORMATION_SOURCE_URL);
		String result = sourceURL;

		String xPath = ".//a[@href = text() or concat('http://', .) = @href or contains(translate(., 'WEBSITE', 'website'), 'website')]";
		List<HtmlAnchor> links = (List<HtmlAnchor>)addressComponent.getByXPath(xPath);

		if (links.size() > 0) {
			result = WebUtils.getLinkedURL(links.get(0));
		}

		return result;
	}

	protected String findImageURL(HtmlElement addressComponent) {
		String result = null;

		DomNodeList<HtmlElement> images = addressComponent.getElementsByTagName("img");

		if (!images.isEmpty()) {
			// TODO improve this, add another step to detect proper image?
			result = images.get(0).getAttribute("src");
		}

		return result;
	}

	protected String findName(HtmlElement addressComponent) {
		// TODO improve this, see e.g. http://www.hapsalons.nl/regio/overijssel/enschede/broodjes/
		Node titleNode = WebUtils.findMostImportantHeader(addressComponent);

		if (titleNode == null) {
			return null;
		} else {
			return titleNode.getTextContent();
		}
	}

	protected String findPhoneNumber(HtmlElement htmlElement, AddressDetector addressDetector) {
		List<String> phoneNumbers = addressDetector.findPhoneNumbers(htmlElement);

		if (phoneNumbers.size() > 0) {
			return phoneNumbers.get(0);
		}

		return null;
	}

	protected void findAddress(PointOfInterest poi, HtmlElement addressComponent, AddressDetector addressDetector) {
		for (DomNode descendant : addressComponent.getDescendants()) {
			String trimmedTextContent = descendant.getTextContent().trim();

			if (addressDetector.isStreetNameAndHouseNumber(trimmedTextContent)) {
				AddressHelper.parseStreetnameAndHouseNumber(poi, trimmedTextContent);
			}
		}
	}

	protected String findPostalCode(HtmlElement htmlElement, AddressDetector addressDetector) {
		List<String> postalCodes = addressDetector.findPostalCodes(htmlElement);

		if (postalCodes.size() > 0) {
			return postalCodes.get(0);
		} else {
			return null;
		}
	}

	protected String findTownName(ScraperMessage<HtmlElement> inputMessage, String postalCode, HtmlElement addressComponent) {
		String queriedTownName = ((Town)inputMessage.getProperty(Town.class)).getName();

		String postalCodeTownName = postalCode + " " + StringUtils.toFirstUpper(queriedTownName);
		String xPath = ".//*[contains(normalize-space(.), '" + postalCodeTownName + "')]";

		if (!addressComponent.getByXPath(xPath).isEmpty()) {
			return StringUtils.toFirstUpper(queriedTownName);
		} else {
			xPath = ".//*[contains(normalize-space(.), '" + postalCode + " ')]";

			@SuppressWarnings("unchecked")
			List<HtmlElement> elements = (List<HtmlElement>)addressComponent.getByXPath(xPath);

			if (!elements.isEmpty()) {
				HtmlElement element = elements.get(elements.size() - 1);
				String textContent = element.getTextContent();

				String townLine = textContent.substring(textContent.indexOf(postalCode) + postalCode.length()).trim();

				if (townLine.contains("\n")) {
					townLine = townLine.substring(0, townLine.indexOf("\n"));
				}

				String townName = townLine.trim();
				return StringUtils.toFirstUpper(townName);
			}
		}

		return null;
	}
}
