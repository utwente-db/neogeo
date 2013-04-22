package nl.utwente.db.neogeo.scraper.utils;

import junit.framework.Assert;

import org.junit.Ignore;

// TODO: add a page, check, then delete it again for proper testing
public class ScraperUtilsTest {

	@Ignore("Only run this with a working database")
	public void hasBeenScraped() {
		Assert.assertTrue(ScraperUtils.hasBeenScraped("http://www.detelefoongids.nl/bg-l/183558140030-Fysiotherapie+De+Haere/vermelding/"));
		ScraperUtils.hasBeenScraped("http://www.detelefoongids.nl/bg-l/18744667-DCW/vermelding/");
		Assert.assertFalse(ScraperUtils.hasBeenScraped("http://www.utwente.nl"));
	}
}
