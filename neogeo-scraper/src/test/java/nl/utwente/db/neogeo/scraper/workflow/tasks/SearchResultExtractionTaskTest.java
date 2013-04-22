package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.messages.BaseScraperMessage;
import nl.utwente.db.neogeo.scraper.utils.ScraperUtils;
import nl.utwente.db.neogeo.utils.FileUtils;

import org.junit.Ignore;

import com.gargoylesoftware.htmlunit.StringWebResponse;
import com.gargoylesoftware.htmlunit.html.HTMLParser;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

@Ignore("Requires network connection")
public class SearchResultExtractionTaskTest extends BaseScraperTaskTest<HtmlPage, HtmlElement> {

	public SearchResultExtractionTaskTest() {
		super(new SearchResultExtractionTask());
	}

	@Override
	public void checkResults() {
		// Make sure there are 25 search results on this page
		int count = 0;

		while (scraperTask.hasNext()) {
			scraperTask.next();
			count++;
		}

		Assert.assertEquals(25, count);
	}

	public Iterator<ScraperMessage<HtmlPage>> createInputIterator() {
		List<ScraperMessage<HtmlPage>> result = new ArrayList<ScraperMessage<HtmlPage>>();
		URL urlObject = null;

		try {
			urlObject = new URL("http://www.detelefoongids.nl/bg/plaats-enschede/w-restaurant/1/");
		} catch (MalformedURLException e) {
			Assert.fail("Unexpected MalformedURLException in hard-coded URL");
		}

		String searchResultPage = FileUtils.getFileAsString("SearchResultPage.html");
		StringWebResponse response = new StringWebResponse(searchResultPage, urlObject);

		HtmlPage page = null;

		try {
			page = HTMLParser.parseHtml(response, ScraperUtils.getRandomWebClient(urlObject).getCurrentWindow());
		} catch (IOException e) {
			Assert.fail("IOException while parsing HTML response" + response);
		}

		ScraperMessage<HtmlPage> scraperMessage = new BaseScraperMessage<HtmlPage>(page);

		scraperMessage.setProperty(SearchResultDetectionTask.SEARCH_RESULT_XPATH_PROPERTY_NAME, "//tbody[./tr/td/span/a]");

		result.add(scraperMessage);

		return result.iterator();
	}
}