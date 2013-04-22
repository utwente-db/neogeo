package nl.utwente.db.neogeo.scraper;

import java.util.Queue;

public interface ScraperWorkflowExecution<OutputType> {
	public void start();
	public void stop();
	
	public boolean isExecutable();
	public double getProgress();
	
	public void setScraperWorkflow(ScraperWorkflow<OutputType> scraperWorkflow);
	public ScraperWorkflow<OutputType> getScraperWorkflow();
	
	public Queue<OutputType> getOutputQueue();
	public void setOutputQueue(Queue<OutputType> outputQueue);
}