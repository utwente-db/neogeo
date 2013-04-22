package nl.utwente.db.neogeo.scraper.workflow.tasks;

import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;
import nl.utwente.db.neogeo.core.model.Town;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.utils.ScraperUtils;

import com.gargoylesoftware.htmlunit.WebRequest;

public class GoudenGidsResourceDetectionTask extends AbstractScraperTask<String, WebRequest> {
	public ScraperMessage<WebRequest> next() {
		ScraperMessage<String> scraperMessage = getNextInputMessage();
		
		String categoryName = ((PointOfInterestCategory)scraperMessage.getProperty(PointOfInterestCategory.class)).getName();
		String townName = ((Town)scraperMessage.getProperty(Town.class)).getName();

		String url = "http://www.detelefoongids.nl/bg/plaats-" + townName.toLowerCase() + "/w-" + categoryName.toLowerCase() + "/1/";
		WebRequest request = ScraperUtils.createWebRequest(url);

		return createScraperMessage(request);
	}
}