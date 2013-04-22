package nl.utwente.db.neogeo.scraper.xpathdetectors;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import nl.utwente.db.neogeo.scraper.utils.ScraperUtils;
import nl.utwente.db.neogeo.utils.test.NeoGeoUnitTest;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class AddressDetectorTest extends NeoGeoUnitTest {

	@Ignore("Slow, requires network connection")
//	@Test
	public void detect() throws Exception {
		Map<String, String> expectedXPaths = new HashMap<String, String>();

		// Items with an ID attribute
		expectedXPaths.put("http://www.inrotterdamuit.nl/restaurant/de_unie.html", "//div[@id='itemDetail']");
		expectedXPaths.put("http://www.subway.nl/storelocator/storelocator.php?city=39&store=50&lang=nl", "//div[@id='details']");
		expectedXPaths.put("http://www.wiel-rent.nl/contact", "//div[@id='contact_address']");
		expectedXPaths.put("http://www.detelefoongids.nl/bg-l/12767522-Stuivenberg+Rijwielen-Verlichting+Ton/vermelding/", "//div[@id='contactInfo']");
		expectedXPaths.put("http://www.aannemer-meijerink.nl/", "//div[@id='contact']");

		// Items without an ID attribute
		expectedXPaths.put("http://www.pathe.nl/bioscoop/schouwburgplein", "//div[@id='content']/div[contains(@class, 'content-side')]/div[contains(@class, 'biosInfo')]/div[contains(@class, 'biosBody')]/p");
		expectedXPaths.put("http://www.musea.nl/index.cfm/museum/Museum-voor-Keramiek-Pablo-Rueda-Lara", "//div[@id='body']/div[5]/div[contains(@class, 'content_item')]/p[7]");
		expectedXPaths.put("http://www.eurocottage.com/", "//div[@id='logobar']/div[contains(@class, 'topcall')]");
		expectedXPaths.put("http://www.onstage.nl/onstage/0,2083,1606_1129%5E1132%5E1404%5E1407%5E1410-039ROT,00.html", "/html/body/table/tbody/tr/td/table/tbody/table[2]/tbody/tr/td[3]/table/tbody/tr[4]/td/table/tbody");
		expectedXPaths.put("http://www.coop.nl/supermarkten/enschede/coopcompact-oude-groen-/", "//form[@id='aspnetForm']/div[contains(@class, 'container')]/div[contains(@class, 'content-container')]/div[contains(@class, 'wrapper')]/div[contains(@class, 'grid12-12')]/div[contains(@class, 'searchstore')]/div[contains(@class, 'openingHoursBlock')]/div[contains(@class, 'contactInformationBlock')]/div[contains(@class, 'storeaddress')]/p");
		expectedXPaths.put("http://gooischerestaurants.nl/plaats/plaats/restaurants-in-hilversum/brasserie-sparkling.html", "//div[@id='listing']/div[contains(@class, 'second')]/div[contains(@class, 'fields')]");
		expectedXPaths.put("http://www.oonk.nl/nieuwsflits/nieuwsflits/high-tea.html", "//table[@id='Tabel_01']/tbody/tr/td[1]/table/tbody/tr[2]/td/div[contains(@class, 'style2')]/table/tbody");
		expectedXPaths.put("http://www.telefoonboek.nl/bedrijven/t2185241/enschede/van-der-veen-auto--electric-b.v./", "//div[@id='searchDetail']/div[contains(@class, 'columnheader_wide')]/div[contains(@class, 'headerTitle')]/div[contains(@class, 'resultdetailcolleft')]/div[contains(@class, 'block')]/div[contains(@class, 'vcard')]");
		expectedXPaths.put("http://www.ikea.com/nl/nl/store/barendrecht/store_info", "//div[@id='storeInfoContainer']/div[contains(@class, 'infoBlock')]/div[contains(@class, 'leftColumn')]/div[2]");
		expectedXPaths.put("http://www.b9.nl/detail.htm?pid=32981", "/html/body/table[2]/tbody/tr[1]/td[2]/table[1]/tbody/tr[2]/td/table/tbody");
		expectedXPaths.put("http://www.hapsalons.nl/regio/overijssel/enschede/broodjes/3497/bakker-bart/", "//div[@id='content']/div[contains(@class, 'column')]/div[contains(@class, 'vcard')]/div[3]");
		
		for (Entry<String, String> test : expectedXPaths.entrySet()) {
			String url = test.getKey();
			String expectedOutput = test.getValue();
			
			logger.info("Detecting address block for " + url);
			
			HtmlPage page = ScraperUtils.toHtmlPage(new WebRequest(new URL(url)));

			AddressDetector addressDetector = new AddressDetector();
			String result = addressDetector.detect(page);
			
			Assert.assertNotNull(result);
			Assert.assertEquals(expectedOutput, result);
		}
	}
	
	@Test
	public void isStreetNameAndHouseNumber() {
		AddressDetector addressDetector = new AddressDetector();

		// Netherlands
		Assert.assertTrue(addressDetector.isStreetNameAndHouseNumber("Oude Markt 15/17"));
		Assert.assertTrue(addressDetector.isStreetNameAndHouseNumber("Noorderhagen 54/A"));
		Assert.assertTrue(addressDetector.isStreetNameAndHouseNumber("Oude Markt 9/A"));
		Assert.assertTrue(addressDetector.isStreetNameAndHouseNumber("Wesseler-nering 2"));
		Assert.assertTrue(addressDetector.isStreetNameAndHouseNumber("Drienerlolaan 5"));

		Assert.assertFalse(addressDetector.isStreetNameAndHouseNumber("Noorderhagen"));
		Assert.assertFalse(addressDetector.isStreetNameAndHouseNumber("Noorderhagen54"));
		Assert.assertFalse(addressDetector.isStreetNameAndHouseNumber("Drienerlolaan: 5"));
		Assert.assertFalse(addressDetector.isStreetNameAndHouseNumber("7522NB Enschede-Noord"));

		// Germany
		Assert.assertTrue(addressDetector.isStreetNameAndHouseNumber("Wittelsbacher Straße 11"));
		Assert.assertTrue(addressDetector.isStreetNameAndHouseNumber("Wittelsbacher St. 11"));

		Assert.assertFalse(addressDetector.isStreetNameAndHouseNumber("48619 Heek"));

		// UK
		Assert.assertTrue(addressDetector.isStreetNameAndHouseNumber("10, Downing Street"));
		Assert.assertTrue(addressDetector.isStreetNameAndHouseNumber("10, Downing St."));

		Assert.assertFalse(addressDetector.isStreetNameAndHouseNumber("London SW1A 2AA"));
	}
}
