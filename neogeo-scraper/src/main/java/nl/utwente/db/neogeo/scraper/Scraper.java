package nl.utwente.db.neogeo.scraper;

import java.util.ArrayList;
import java.util.List;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.core.model.ModelObject;
import nl.utwente.db.neogeo.scraper.workflow.BaseWorkflowExecution;
import nl.utwente.db.neogeo.utils.CommandLineToolUtils;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThread;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThreadPool;

import org.apache.log4j.Logger;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

public class Scraper<OutputType> {
	private ScraperWorkflowExecution<OutputType> workflowExecution;
	private Logger logger = Logger.getLogger(Scraper.class);

	public Scraper(ScraperWorkflow<OutputType> scraperWorkflow) {
		this.workflowExecution = new BaseWorkflowExecution<OutputType>();
		workflowExecution.setScraperWorkflow(scraperWorkflow);
	}
	
	public static void main(String[] args) {
		String scraperWorkflowClassName = CommandLineToolUtils.getOption(args, "workflow");
		ScraperWorkflow<ModelObject> scraperWorkflow = getScraperWorkflow(scraperWorkflowClassName);
		CommandLineToolUtils.parseOptions(args, scraperWorkflow);

		Scraper<ModelObject> scraper = new Scraper<ModelObject>(scraperWorkflow);
		ScraperRunnable<ModelObject> scraperRunnable = new ScraperRunnable<ModelObject>(scraper);

		MonitoredThread<?> thread = MonitoredThreadPool.getInstance().createMonitoredThread(scraperRunnable);
		thread.start();
	}
	
	@SuppressWarnings("unchecked")
	public static ScraperWorkflow<ModelObject> getScraperWorkflow(String className) {
		Class<ScraperWorkflow<ModelObject>> scraperWorkflowClass;
		
		if (className == null) {
			throw new NullPointerException("className cannot be null");
		}
		
		if (!className.contains(".")) {
			className = "nl.utwente.db.neogeo.scraper.workflow.examples." + className;
		}
		
		try {
			scraperWorkflowClass = (Class<ScraperWorkflow<ModelObject>>)Class.forName(className);
		} catch (ClassNotFoundException e) {
			throw new NeoGeoException("Could not find workflow class: " + className);
		}
		
		BeanWrapper workflowWrapper = new BeanWrapperImpl(scraperWorkflowClass);
		
		return (ScraperWorkflow<ModelObject>)workflowWrapper.getWrappedInstance();
	}
	
	public void start() {
		if (!workflowExecution.isExecutable()) {
			logger.fatal("Unable to start scraper: workflowExecution " + workflowExecution + " still needs to be initialized.");
	
			System.out.println("Unable to start scraper. See log for details.");
			printUsage();
			
			return;
		}
		
		try {
			logger.info("Starting scraperWorkflow: " + workflowExecution.getScraperWorkflow());

			workflowExecution.start();

			logger.info("Executed workflow: " + workflowExecution.getScraperWorkflow() + " successfully");
		} catch (Exception e) {
			logger.fatal("Unable to complete workflowExecution: " + workflowExecution, e);
		}
	}
	
	public void stop() {
		this.workflowExecution.stop();
	}

	protected void printUsage() {
		List<String> usedPrefixes = new ArrayList<String>();
		System.out.println(CommandLineToolUtils.generateUsageDescription(this, usedPrefixes));
		
		if (workflowExecution.getScraperWorkflow() == null) {
			System.out.println(CommandLineToolUtils.generateUsageDescription(workflowExecution.getScraperWorkflow(), "  ", usedPrefixes));
		}
	}

	public ScraperWorkflowExecution<OutputType> getWorkflowExecution() {
		return workflowExecution;
	}

	public void setWorkflowExecution(ScraperWorkflowExecution<OutputType> workflowExecution) {
		this.workflowExecution = workflowExecution;
	}

}
