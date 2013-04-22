package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.utils.ScraperUtils;
import nl.utwente.db.neogeo.utils.WebUtils;

import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlElement;

public class DetailLinkExtractionTask extends AbstractScraperTask<HtmlElement, WebRequest> {

	protected Set<String> scrapedURLs = new HashSet<String>();

	public ScraperMessage<WebRequest> next() {
		String linkedURL = getNextLinkedURL();
		
		while (scrapedURLs.contains(linkedURL) || ScraperUtils.hasBeenScraped(linkedURL)) {
			linkedURL = getNextLinkedURL();
		}
		
		scrapedURLs.add(linkedURL);
		WebRequest request = ScraperUtils.createWebRequest(linkedURL);

		return createScraperMessage(request);
	}

	protected String getNextLinkedURL() {
		ScraperMessage<HtmlElement> inputMessage = getNextInputMessage();
		HtmlElement searchResultComponent = inputMessage.getBody();

		String xPath = (String)inputMessage.getProperty(DetailLinkDetectionTask.DETAIL_LINK_XPATH_PROPERTY_NAME);

		@SuppressWarnings("unchecked")
		List<HtmlAnchor> linkElements = (List<HtmlAnchor>)searchResultComponent.getByXPath("." + xPath);

		String linkedURL = "";

		if (!linkElements.isEmpty()) {
			linkedURL = linkElements.get(0).getHrefAttribute();
		}
		
		URL originalURL = (URL)inputMessage.getProperty(SearchResultPaginationTask.SEARCH_RESULT_SOURCE_URL);
		return WebUtils.getLinkedURL(originalURL, linkedURL);
	}
}
