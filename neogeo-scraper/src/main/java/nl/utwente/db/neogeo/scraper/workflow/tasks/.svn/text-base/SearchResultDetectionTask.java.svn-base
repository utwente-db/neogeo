package nl.utwente.db.neogeo.scraper.workflow.tasks;

import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.srf.SearchResultDetectionService;

import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class SearchResultDetectionTask extends AbstractScraperTask<HtmlPage, HtmlPage> {
	public final static String SEARCH_RESULT_XPATH_PROPERTY_NAME = SearchResultDetectionTask.class.getCanonicalName() + ".xpath";
	
	private SearchResultDetectionService searchResultDetectionService;
	private String xPath;

	public SearchResultDetectionTask() {
	}
	
	public SearchResultDetectionTask(SearchResultDetectionService service) {
		this.searchResultDetectionService = service;
	}

	public ScraperMessage<HtmlPage> next() {
		ScraperMessage<HtmlPage> input = getNextInputMessage();

		if (xPath == null) {
			String url = input.getProperty(SearchResultPaginationTask.SEARCH_RESULT_SOURCE_URL).toString();
			
			if (searchResultDetectionService == null) {
				throw new ScraperException("Unable to detect xPath for search results, no SearchResultDetectionService configured");
			}
			
			xPath = searchResultDetectionService.detect(url);
			
			if (xPath == null) {
				throw new ScraperException("Unable to determine SearchResult xPath for " + url);
			}
		}

		input.setProperty(SEARCH_RESULT_XPATH_PROPERTY_NAME, xPath);

		return input;
	}

	public SearchResultDetectionService getSearchResultDetectionService() {
		return searchResultDetectionService;
	}

	public void setSearchResultDetectionService(
			SearchResultDetectionService searchResultDetectionService) {
		this.searchResultDetectionService = searchResultDetectionService;
	}
}