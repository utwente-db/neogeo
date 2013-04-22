package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.util.ArrayList;
import java.util.Iterator;

import nl.utwente.db.neogeo.core.model.ModelObject;
import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.db.utils.NeoGeoDBUtils;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.messages.BaseScraperMessage;

import org.junit.Ignore;

@Ignore("This test does not clean up after itself yet. A possibility would be to filter based on the name prefix")
public class PersistenceTaskTest extends BaseScraperTaskTest<ModelObject, ModelObject> {

	public PersistenceTaskTest() {
		super(new PersistenceTask());
	}

	public void checkResults() {
		while (scraperTask.hasNext()) {
			scraperTask.next();
		}
	}

	public Iterator<ScraperMessage<ModelObject>> createInputIterator() {
		ArrayList<ScraperMessage<ModelObject>> inputList = new ArrayList<ScraperMessage<ModelObject>>();

		PointOfInterest poi = new PointOfInterest();
		ScraperMessage<ModelObject> poiMessage = new BaseScraperMessage<ModelObject>(poi);

		poi.setHouseNumber("5");
		poi.setStreetName("Drienerlolaan");
		poi.setName("TEST: Universiteit Twente");

		poi.setCategory(NeoGeoDBUtils.getCategoryByName("universiteit"));
		poi.setPhoneNumber("053-4899111");

		poi.setPostalCode("7522NB");
		poi.setTown(NeoGeoDBUtils.getTownByName("Enschede"));

		poi.setSourceUrl("http://www.detelefoongids.nl/bg-l/180878780010-Universiteit+Twente/vermelding/");

		inputList.add(poiMessage);

		return inputList.iterator();
	}
}