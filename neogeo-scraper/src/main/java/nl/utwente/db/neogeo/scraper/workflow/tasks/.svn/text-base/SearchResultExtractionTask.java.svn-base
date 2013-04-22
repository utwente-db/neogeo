package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.util.List;

import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.scraper.ScraperMessage;

import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class SearchResultExtractionTask extends AbstractScraperTask<HtmlPage, HtmlElement> {
	private int index = 0;
	private List<HtmlElement> elementList;

	public ScraperMessage<HtmlElement> next() {
		HtmlElement searchResult = getNextSearchResult();
		
		while (searchResult == null) {
			searchResult = getNextSearchResult();
		}

		return createScraperMessage(searchResult);
	}

	@SuppressWarnings("unchecked")
	public HtmlElement getNextSearchResult() {
		if (elementList == null || index >= elementList.size()) {
			// Go to next search result page
			ScraperMessage<HtmlPage> inputMessage = getNextInputMessage();

			HtmlPage page = inputMessage.getBody();
			String xPath = (String)inputMessage.getProperty(SearchResultDetectionTask.SEARCH_RESULT_XPATH_PROPERTY_NAME);

			elementList = (List<HtmlElement>)page.getByXPath(xPath);
			index = 0;
		}

		if (elementList.isEmpty()) {
			// No search results
			if (hasNext()) {
				return null;
			} else {
				throw new ScraperException("No more search results, abort scraping.");
			}
		}
		
		return elementList.get(index++);
	}

	@Override
	public boolean hasNext() {
		return ((elementList != null && index < elementList.size()) || super.hasNext());
	}
}