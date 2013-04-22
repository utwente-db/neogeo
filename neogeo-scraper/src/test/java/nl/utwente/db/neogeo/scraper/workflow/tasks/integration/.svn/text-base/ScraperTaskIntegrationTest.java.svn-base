package nl.utwente.db.neogeo.scraper.workflow.tasks.integration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.workflow.BaseWorkflow;
import nl.utwente.db.neogeo.scraper.workflow.tasks.ScraperTaskTest;

import org.junit.Before;
import org.junit.Test;

public abstract class ScraperTaskIntegrationTest<InputType, OutputType> {
	@SuppressWarnings("rawtypes")
	private BaseWorkflow<?> baseWorkflow = new BaseWorkflow();
	private Map<Class<? extends ScraperTaskTest<? extends Object, ? extends Object>>, ScraperTaskTest<? extends Object, ? extends Object>> tests =
	 new HashMap<Class<? extends ScraperTaskTest<? extends Object,? extends Object>>, ScraperTaskTest<? extends Object,? extends Object>>();
	
	@Before
	public abstract void setUp();
	public abstract void checkResults();
	
	@Test
	public void test() {
		Assert.assertTrue(getLastIterator().hasNext());
		checkResults();
	}
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void addTest(ScraperTaskTest<? extends Object, ? extends Object> scraperTaskTest) {
		if (!baseWorkflow.hasTasks()) {
			baseWorkflow.setLastIterator((Iterator)scraperTaskTest.createInputIterator());
		}
		
		baseWorkflow.addTask(scraperTaskTest.getScraperTask());
		this.tests.put((Class<? extends ScraperTaskTest<? extends Object,? extends Object>>)scraperTaskTest.getClass(), (ScraperTaskTest<? extends Object,? extends Object>)scraperTaskTest);
	}
	
	protected ScraperTaskTest<? extends Object,? extends Object> getTest(Class<? extends ScraperTaskTest<? extends Object, ? extends Object>> clazz) {
		return this.tests.get(clazz);
	}

	public BaseWorkflow<?> getBaseWorkflow() {
		return baseWorkflow;
	}

	public void setBaseWorkflow(BaseWorkflow<?> baseWorkflow) {
		this.baseWorkflow = baseWorkflow;
	}
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	public Iterator<ScraperMessage<OutputType>> getLastIterator() {
		return (Iterator)baseWorkflow.getLastIterator();
	}
}
