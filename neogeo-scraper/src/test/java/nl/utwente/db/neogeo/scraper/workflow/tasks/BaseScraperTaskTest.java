package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.util.Iterator;

import junit.framework.Assert;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.ScraperTask;

import org.junit.Before;
import org.junit.Test;

public abstract class BaseScraperTaskTest<InputType extends Object, OutputType extends Object> implements ScraperTaskTest<InputType, OutputType> {
	protected final ScraperTask<InputType, OutputType> scraperTask;

	public BaseScraperTaskTest(ScraperTask<InputType, OutputType> scraperTask) {
		this.scraperTask = scraperTask;
	}
	
	@Before
	public void init() {
		scraperTask.setInputIterator(this.createInputIterator());
	}
	
	@Test
	public void test() {
		Assert.assertTrue(scraperTask.hasNext());
		
		this.checkResults();
	}
	
	public abstract void checkResults();
	public abstract Iterator<ScraperMessage<InputType>> createInputIterator();

	public ScraperTask<InputType, OutputType> getScraperTask() {
		return scraperTask;
	}
}
