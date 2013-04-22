package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.util.Iterator;

import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.ScraperTask;

public interface ScraperTaskTest<InputType extends Object, OutputType extends Object> {
	public void test();

	public ScraperTask<InputType, OutputType> getScraperTask();
	public Iterator<ScraperMessage<InputType>> createInputIterator();
	
	public void checkResults();
}
