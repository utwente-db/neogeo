package nl.utwente.db.neogeo.scraper.workflow.examples;

import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.scraper.srf.WebDriverSearchResultDetectionService;
import nl.utwente.db.neogeo.scraper.workflow.BaseWorkflow;
import nl.utwente.db.neogeo.scraper.workflow.tasks.DetailLinkDetectionTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.DetailLinkExtractionTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.DetailedInformationDetectionTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.DetailedInformationExtractionTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.GoudenGidsResourceDetectionTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.PersistenceTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.SearchResultDetectionTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.SearchResultExtractionTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.SearchResultPaginationTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.geo.GeoDetailedInformationMappingTask;
import nl.utwente.db.neogeo.scraper.workflow.tasks.geo.GeoSpecificTownSearchSpaceTask;
import nl.utwente.db.neogeo.scraper.xpathdetectors.AddressDetector;
import nl.utwente.db.neogeo.utils.StringUtils;

public class GoudenGidsWorkflow extends BaseWorkflow<PointOfInterest> {
	private String firstCategory;
	private String town;

	@Override
	public boolean init() {
		addTask(new GeoSpecificTownSearchSpaceTask(getTown(), getFirstCategory()));
		
		addTask(new GoudenGidsResourceDetectionTask());
		addTask(new SearchResultPaginationTask());

		addTask(new SearchResultDetectionTask(new WebDriverSearchResultDetectionService()));
		addTask(new SearchResultExtractionTask());

		// TODO implement my algorithm, currently hard-coded xPaths
		addTask(new DetailLinkDetectionTask());
		addTask(new DetailLinkExtractionTask());

		// TODO improve xPath's?
		addTask(new DetailedInformationDetectionTask(new AddressDetector()));
		// TODO first item returned. is this right?
		addTask(new DetailedInformationExtractionTask());

		// TODO More generic solution?
		addTask(new GeoDetailedInformationMappingTask());
		addTask(new PersistenceTask());
	
		return true;
	}

	public String getTown() {
		return town;
	}

	public void setTown(String town) {
		this.town = town;
	}

	public String getFirstCategory() {
		return firstCategory;
	}

	public void setFirstCategory(String firstCategory) {
		this.firstCategory = firstCategory;
	}
	
	@Override
	public boolean isExecutable() {
		return !StringUtils.isEmpty(town) && super.isExecutable();
	}
		
	@Override
	public String toString() {
		return "GoudenGidsWorkflow [firstCategory=" + firstCategory + ", town="
				+ town + "]";
	}

}