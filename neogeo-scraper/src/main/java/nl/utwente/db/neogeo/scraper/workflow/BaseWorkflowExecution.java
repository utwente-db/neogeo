package nl.utwente.db.neogeo.scraper.workflow;

import java.util.Queue;

import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.ScraperWorkflow;
import nl.utwente.db.neogeo.scraper.ScraperWorkflowExecution;

import org.apache.log4j.Logger;

public class BaseWorkflowExecution<OutputType> implements ScraperWorkflowExecution<OutputType> {
	protected ScraperWorkflow<OutputType> scraperWorkflow = null;
	protected boolean stopped = false;
	
	protected Logger logger = Logger.getLogger(BaseWorkflowExecution.class);
	private Queue<OutputType> outputQueue = null;

	public void start() {
		if (!this.isExecutable()) {
			throw new ScraperException("Unable to start workflow: " + this);
		}
		
		while (!this.stopped && scraperWorkflow.getLastIterator().hasNext()) {
			try {
				ScraperMessage<OutputType> output = scraperWorkflow.getLastIterator().next();
				
				if (getOutputQueue() != null) {
					getOutputQueue().add(output.getBody());
				}
				
				logger.info(output.getBody());
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Exception while executing " + this.getClass().getSimpleName(), e);
			}
		}
	}
	
	public void stop() {
		this.stopped  = true;
	}
	
	public boolean isExecutable() {
		return scraperWorkflow.isExecutable();
	}
	
	public double getProgress() {
		if (scraperWorkflow.getInitialScraperTask() == null) {
			return 0;
		}
		
		return scraperWorkflow.getInitialScraperTask().getProgress();
	}
	
	public void setScraperWorkflow(ScraperWorkflow<OutputType> scraperWorkflow) {
		this.scraperWorkflow = scraperWorkflow;
	}
	
	public ScraperWorkflow<OutputType> getScraperWorkflow() {
		return scraperWorkflow;
	}

	public Queue<OutputType> getOutputQueue() {
		return outputQueue;
	}
	
	public void setOutputQueue(Queue<OutputType> outputQueue) {
		this.outputQueue = outputQueue;
	}
}