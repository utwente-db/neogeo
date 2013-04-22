package nl.utwente.db.neogeo.scraper.workflow;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.ScraperTask;
import nl.utwente.db.neogeo.scraper.ScraperWorkflow;
import nl.utwente.db.neogeo.scraper.workflow.tasks.InitialScraperTask;

public class BaseWorkflow<OutputType> implements ScraperWorkflow<OutputType> {
	protected Iterator<ScraperMessage<OutputType>> lastIterator;
	
	private Map<Class<? extends ScraperTask<? extends Object, ? extends Object>>, ScraperTask<? extends Object, ? extends Object>> tasks =
	 new HashMap<Class<? extends ScraperTask<? extends Object, ? extends Object>>, ScraperTask<? extends Object, ? extends Object>>();
	private InitialScraperTask<? extends Object, ? extends Object> initialScraperTask;
	
	public ScraperTask<? extends Object, ? extends Object> getTask(Class<? extends ScraperTask<? extends Object, ? extends Object>> clazz) {
		return this.tasks.get(clazz);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked"})
	public void addTask(ScraperTask<? extends Object, ? extends Object> scraperTask) {
		if (scraperTask instanceof InitialScraperTask) {
			initialScraperTask = (InitialScraperTask)scraperTask;
		}
		
		scraperTask.setInputIterator((Iterator)this.getLastIterator());
		scraperTask.setScraperWorkflow(this);
		
		this.setLastIterator((Iterator)scraperTask);
		this.tasks.put((Class<? extends ScraperTask<? extends Object, ? extends Object>>)scraperTask.getClass(), scraperTask);
	}
	
	public boolean hasTasks() {
		return getLastIterator() != null;
	}

	public Iterator<ScraperMessage<OutputType>> getLastIterator() {
		return lastIterator;
	}

	public void setLastIterator(Iterator<ScraperMessage<OutputType>> lastIterator) {
		this.lastIterator = lastIterator;
	}

	public boolean isExecutable() {
		return hasTasks() || init();
	}
	
	/**
	 * Overwrite this method to initialize a hardcoded workflow. Resulting boolean indicates if the initialization was successful.
	 */
	public boolean init() {
		return hasTasks();
	}

	public InitialScraperTask<? extends Object, ? extends Object> getInitialScraperTask() {
		return initialScraperTask;
	}
}
