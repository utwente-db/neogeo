package nl.utwente.db.neogeo.scraper.admin.servlets;

import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.scraper.Scraper;
import nl.utwente.db.neogeo.scraper.ScraperRunnable;
import nl.utwente.db.neogeo.scraper.workflow.examples.GoudenGidsWorkflow;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.MonitoredThreadResponse;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.start.MonitoredThreadStartServlet;
import nl.utwente.db.neogeo.utils.tasks.MonitoredRunnable;

public class ScraperStartServlet extends MonitoredThreadStartServlet<ScraperStartRequest, MonitoredThreadResponse> {
	private static final long serialVersionUID = 1L;

	@Override
	public MonitoredRunnable<?> createRunnable(ScraperStartRequest request) {
		// TODO Make the workflow and its options configurable
		GoudenGidsWorkflow workflow = new GoudenGidsWorkflow();
		Scraper<PointOfInterest> scraper = new Scraper<PointOfInterest>(workflow);
		
		workflow.setTown("enschede");

		return new ScraperRunnable<PointOfInterest>(scraper);
	}
}