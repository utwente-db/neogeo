package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.util.ArrayList;
import java.util.Iterator;

import junit.framework.Assert;
import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;
import nl.utwente.db.neogeo.core.model.Town;
import nl.utwente.db.neogeo.db.utils.NeoGeoDBUtils;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.messages.BaseScraperMessage;
import nl.utwente.db.neogeo.scraper.utils.ScraperUtils;
import nl.utwente.db.neogeo.scraper.workflow.tasks.geo.GeoDetailedInformationMappingTask;
import nl.utwente.db.neogeo.utils.FileUtils;

import org.junit.Ignore;

import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

@Ignore("Requires Database connection")
public class GeoDetailedInformationMappingTaskTest extends BaseScraperTaskTest<HtmlElement, PointOfInterest> {
	
	public GeoDetailedInformationMappingTaskTest() {
		super(new GeoDetailedInformationMappingTask());
	}

	public void checkResults() {
		checkFirstResult();
		checkSecondResult();
	}
	
	public Iterator<ScraperMessage<HtmlElement>> createInputIterator() {
		ArrayList<ScraperMessage<HtmlElement>> inputList = new ArrayList<ScraperMessage<HtmlElement>>();

		ScraperMessage<HtmlElement> scraperMessage = createScraperMessage("http://www.detelefoongids.nl/bg-l/18688539-Hu%27s+Garden+Chinees-Aziatisch+Restaurant/vermelding/", "DetailedInformationPage.html", "//div[@id='contactInfo']", "restaurant", "Enschede");
		inputList.add(scraperMessage);

		ScraperMessage<HtmlElement> scraperMessage2 = createScraperMessage("http://www.detelefoongids.nl/bg-l/19355271-Packhuys+Restaurant+%27t/vermelding/", "DetailedInformationPage2.html", "//div[@id='contactInfo']", "restaurant", "Enschede");
		inputList.add(scraperMessage2);

		return inputList.iterator();
	}
	
	private void checkFirstResult() {
		PointOfInterest poi = scraperTask.next().getBody();

		Assert.assertNotNull(poi);
		Assert.assertEquals("Hu's Garden Chinees-Aziatisch Restaurant", poi.getName());

		Assert.assertEquals("Oldenzaalsestraat", poi.getStreetName());
		Assert.assertEquals("266", poi.getHouseNumber());

		Assert.assertEquals("7523AG", poi.getPostalCode());
		Assert.assertEquals("Enschede", poi.getTown().getName());

		Assert.assertEquals("0534333678", poi.getPhoneNumber());

		Assert.assertEquals("http://www.husgarden.nl", poi.getUrl());
		Assert.assertEquals("http://www.detelefoongids.nl/images/da/6251365.jpg", poi.getImageUrl());
	}

	private void checkSecondResult() {
		PointOfInterest poi = scraperTask.next().getBody();

		Assert.assertNotNull(poi);
		Assert.assertEquals("Packhuys Restaurant 't", poi.getName());

		Assert.assertEquals("Kinderdijk", poi.getStreetName());
		Assert.assertEquals("84", poi.getHouseNumber());

		Assert.assertEquals("4331HH", poi.getPostalCode());
		Assert.assertEquals("Middelburg", poi.getTown().getName());

		Assert.assertEquals("0118674064", poi.getPhoneNumber());

		Assert.assertNotNull(poi.getUrl());
		Assert.assertNull(poi.getImageUrl());
	}

	private ScraperMessage<HtmlElement> createScraperMessage(String url, String fileName, String elementXPath, String categoryName, String townName) {
		HtmlElement element = createHTMLElement(url, fileName, elementXPath);
		ScraperMessage<HtmlElement> scraperMessage = new BaseScraperMessage<HtmlElement>(element);

		scraperMessage.setProperty(PointOfInterestCategory.class, NeoGeoDBUtils.getCategoryByName(categoryName));
		scraperMessage.setProperty(Town.class, NeoGeoDBUtils.getTownByName(townName));
		scraperMessage.setProperty(DetailedInformationDetectionTask.DETAILED_INFORMATION_SOURCE_URL, url);
		
		return scraperMessage;
	}

	private HtmlElement createHTMLElement(String url, String fileName, String elementXPath) {
		String detailedInformationPage = FileUtils.getFileAsString(fileName);
		HtmlPage page = ScraperUtils.toHtmlPage(url, detailedInformationPage);

		return (HtmlElement)page.getByXPath(elementXPath).get(0);
	}

}