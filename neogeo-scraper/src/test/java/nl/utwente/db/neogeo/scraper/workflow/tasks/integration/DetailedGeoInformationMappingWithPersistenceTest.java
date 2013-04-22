package nl.utwente.db.neogeo.scraper.workflow.tasks.integration;

import junit.framework.Assert;
import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.scraper.workflow.tasks.GeoDetailedInformationMappingTaskTest;
import nl.utwente.db.neogeo.scraper.workflow.tasks.PersistenceTaskTest;

import org.junit.Before;
import org.junit.Ignore;

import com.gargoylesoftware.htmlunit.html.HtmlElement;

@Ignore("This test does not clean up after itself yet. A possibility would be to filter based on the name prefix")
public class DetailedGeoInformationMappingWithPersistenceTest extends ScraperTaskIntegrationTest<HtmlElement, PointOfInterest> {
	
	@Override
	@Before
	public void setUp() {
		this.addTest(new GeoDetailedInformationMappingTaskTest());
		this.addTest(new PersistenceTaskTest());
	}
	
	@Override
	public void checkResults() {
		while (getLastIterator().hasNext()) {
			PointOfInterest poi = getLastIterator().next().getBody();
			
			Assert.assertNotNull(poi);
			
			// TODO inspect poi closer here
		}
	}
}
