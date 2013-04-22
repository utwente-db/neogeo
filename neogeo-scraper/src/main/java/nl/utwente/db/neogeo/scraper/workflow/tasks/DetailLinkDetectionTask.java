package nl.utwente.db.neogeo.scraper.workflow.tasks;

import nl.utwente.db.neogeo.scraper.ScraperMessage;

import com.gargoylesoftware.htmlunit.html.HtmlElement;

public class DetailLinkDetectionTask extends AbstractScraperTask<HtmlElement, HtmlElement> {

	public final static String DETAIL_LINK_XPATH_PROPERTY_NAME = DetailLinkDetectionTask.class.getCanonicalName() + ".xpath";

	public ScraperMessage<HtmlElement> next() {
		ScraperMessage<HtmlElement> inputMessage = getNextInputMessage();

		inputMessage.setProperty(DETAIL_LINK_XPATH_PROPERTY_NAME, "//h2/a"); // TODO detect this dynamically

		return inputMessage;
	}
}
