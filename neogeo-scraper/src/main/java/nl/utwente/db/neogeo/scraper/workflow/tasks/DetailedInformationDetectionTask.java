package nl.utwente.db.neogeo.scraper.workflow.tasks;

import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.utils.ScraperUtils;
import nl.utwente.db.neogeo.scraper.xpathdetectors.XPathDetector;

import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class DetailedInformationDetectionTask extends AbstractScraperTask<WebRequest, HtmlPage> {
	public static final String DETAILED_INFORMATION_SOURCE_URL = DetailedInformationDetectionTask.class.getCanonicalName() + ".url";
	public final static String DETAILED_INFORMATION_XPATH_PROPERTY_NAME = DetailedInformationDetectionTask.class.getCanonicalName() + ".xpath";

	private String xPath;
	private final XPathDetector detector;
	
	public DetailedInformationDetectionTask(XPathDetector detector) {
		this.detector = detector;
	}

	public ScraperMessage<HtmlPage> next() {
		ScraperMessage<WebRequest> scraperMessage = getNextInputMessage();
		HtmlPage htmlPage = ScraperUtils.toHtmlPage(scraperMessage.getBody());

		if (xPath == null) {
			xPath = detector.detect(htmlPage);
		}
		
		if (htmlPage.getByXPath(xPath).isEmpty()) {
			String searchResultXPath = scraperMessage.getStringProperty(SearchResultDetectionTask.SEARCH_RESULT_XPATH_PROPERTY_NAME);
			
			if (htmlPage.getByXPath(searchResultXPath).isEmpty()) {
				// Re-detect xPath
				xPath = detector.detect(htmlPage);
			} else {
				// Search result page
				sendMessage(SearchResultPaginationTask.class, scraperMessage);
			}
		}

		scraperMessage.setProperty(DETAILED_INFORMATION_SOURCE_URL, scraperMessage.getBody().getUrl().toString());
		scraperMessage.setProperty(DETAILED_INFORMATION_XPATH_PROPERTY_NAME, xPath);
		
		return createScraperMessage(htmlPage);
	}

}
