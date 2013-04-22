package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.utils.ScraperUtils;
import nl.utwente.db.neogeo.utils.WebUtils;

import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.html.HtmlAnchor;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class SearchResultPaginationTask extends AbstractScraperTask<WebRequest, HtmlPage> {
	public final static String SEARCH_RESULT_SOURCE_URL = SearchResultPaginationTask.class.getCanonicalName() + ".url";

	private WebRequest nextRequest = null;
	private Set<String> scrapedURLs = new HashSet<String>();

	public ScraperMessage<HtmlPage> next() {
		WebRequest request = nextRequest;

		if (request == null) {
			request = getNextInputMessage().getBody();
		}

		HtmlPage page = null;
		
		try {
			page = ScraperUtils.toHtmlPage(request);
		} catch (Exception e) {
			// Don't keep trying to load that page when it is unavailable.
			nextRequest = null;
			throw new ScraperException("Unable to get HtmlPage for " + request, e);
		}
		
		ScraperMessage<HtmlPage> result = createScraperMessage(page);

		result.setProperty(SEARCH_RESULT_SOURCE_URL, request.getUrl());
		detectNextLink(page, request.getUrl());

		return result;
	}

	@Override
	public boolean hasNext() {
		return nextRequest != null || super.hasNext();
	}

	protected void detectNextLink(HtmlPage page, URL originalURL) {
		// TODO do this language independent
		// TODO what if it is not a link, but an onClick
		// TODO what if it is not a link, but a mini-form
		@SuppressWarnings("unchecked")
		List<HtmlAnchor> links = (List<HtmlAnchor>)page.getByXPath("//a[contains(., 'Volgende')]");

		if (links.isEmpty()) {
			nextRequest = null;
		} else {
			String linkedURL = links.get(0).getHrefAttribute();
			linkedURL = WebUtils.getLinkedURL(originalURL, linkedURL);

			if (scrapedURLs.contains(linkedURL)) {
				return;
			}

			scrapedURLs.add(linkedURL);
			nextRequest = ScraperUtils.createWebRequest(linkedURL);
		}
	}

}