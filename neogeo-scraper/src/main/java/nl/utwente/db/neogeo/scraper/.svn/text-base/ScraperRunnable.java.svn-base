package nl.utwente.db.neogeo.scraper;

import nl.utwente.db.neogeo.utils.tasks.MonitoredRunnable;

public class ScraperRunnable<OutputType> extends MonitoredRunnable<OutputType> {
	private final Scraper<OutputType> scraper;

	public ScraperRunnable(Scraper<OutputType> scraper) {
		this.scraper = scraper;
		
		ScraperWorkflowExecution<OutputType> workflowExecution = scraper.getWorkflowExecution();
		
		workflowExecution.setOutputQueue(getMessageQueue());
	}

	public void run() {
		scraper.start();
	}
	
	public void stop() {
		scraper.stop();
	}

	@Override
	public double getProgress() {
		return scraper.getWorkflowExecution().getProgress();
	}
}
