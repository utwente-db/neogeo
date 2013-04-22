package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.util.List;

import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.scraper.ScraperMessage;

import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class DetailedInformationExtractionTask extends AbstractScraperTask<HtmlPage, HtmlElement> {

	public ScraperMessage<HtmlElement> next() {
		HtmlElement result = getNextDetailElement();
		
		while (result == null) {
			result = getNextDetailElement();
		}

		return createScraperMessage(result);
	}
	
	protected HtmlElement getNextDetailElement() {
		ScraperMessage<HtmlPage> inputMessage = getNextInputMessage();
		String xPath = (String)inputMessage.getProperty(DetailedInformationDetectionTask.DETAILED_INFORMATION_XPATH_PROPERTY_NAME);

		@SuppressWarnings("unchecked")
		List<HtmlElement> elementList = (List<HtmlElement>)inputMessage.getBody().getByXPath(xPath);

		if (elementList.isEmpty()) {
			sendMessage(SearchResultExtractionTask.class, inputMessage);

			if (hasNext()) {
				return null;
			} else {
				throw new ScraperException("No more detail pages, but found a result page as last 'detail' page.");
			}
		}
		
		// Return the first found item
		return elementList.get(0);
	}
}
