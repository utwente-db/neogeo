package nl.utwente.db.neogeo.scraper.workflow.tasks.geo;

import java.util.ArrayList;
import java.util.List;

import nl.utwente.db.neogeo.core.model.PointOfInterestCategory;
import nl.utwente.db.neogeo.core.model.Town;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.workflow.tasks.AbstractScraperTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.InitialScraperTask;

public abstract class GeoSearchSpaceTask extends AbstractScraperTask<String, String> implements InitialScraperTask<String, String> {
	protected List<Town> towns = new ArrayList<Town>();
	protected List<PointOfInterestCategory> poiCategories = new ArrayList<PointOfInterestCategory>();

	protected int townIndex = 0;
	protected int categoryIndex = 0;
	
	public ScraperMessage<String> next() {
		if (categoryIndex == poiCategories.size()) {
			townIndex++;
			categoryIndex = 0;
		}
		
		PointOfInterestCategory category = poiCategories.get(categoryIndex++);
		Town town = towns.get(townIndex);
		
		ScraperMessage<String> scraperMessage = createScraperMessage(town.getName() + ":" + category.getName());

		scraperMessage.setProperty(PointOfInterestCategory.class, category);
		scraperMessage.setProperty(Town.class, town);
		
		return scraperMessage;
	}

	public void addTown(Town town) {
		this.towns.add(town);
	}

	public void addCategory(PointOfInterestCategory category) {
		this.poiCategories.add(category);
	}

	@Override
	public boolean hasNext() {
		return this.townIndex < towns.size() || this.categoryIndex < poiCategories.size();
	}

	public int getCurrentItemNumber() {
		return (townIndex * poiCategories.size()) + categoryIndex;
	}
	
	public int getSize() {
		return towns.size() * poiCategories.size();
	}
	
	public double getProgress() {
		return ((double)getCurrentItemNumber() / getSize());
	}
}
