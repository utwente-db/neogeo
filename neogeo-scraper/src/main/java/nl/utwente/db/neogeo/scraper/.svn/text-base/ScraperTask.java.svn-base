package nl.utwente.db.neogeo.scraper;

import java.util.Iterator;


public interface ScraperTask<InputType, OutputType> extends Iterator<ScraperMessage<? extends OutputType>> {
	public void setScraperWorkflow(ScraperWorkflow<?> scraperWorkflow);
	public void setInputIterator(Iterator<ScraperMessage<InputType>> inputIterator);

	@SuppressWarnings("rawtypes")
	public void addMessage(ScraperMessage scraperMessage);
}