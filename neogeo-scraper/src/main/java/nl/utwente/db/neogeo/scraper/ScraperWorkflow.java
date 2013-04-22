package nl.utwente.db.neogeo.scraper;

import java.util.Iterator;

import nl.utwente.db.neogeo.scraper.workflow.tasks.InitialScraperTask;

public interface ScraperWorkflow<OutputType> {
	public ScraperTask<?, ?> getTask(Class<? extends ScraperTask<?, ?>> clazz);
	public void addTask(ScraperTask<?, ?> scraperTask);
	
	public InitialScraperTask<?, ?> getInitialScraperTask();
	public boolean isExecutable();
	
	public Iterator<ScraperMessage<OutputType>> getLastIterator();
}
