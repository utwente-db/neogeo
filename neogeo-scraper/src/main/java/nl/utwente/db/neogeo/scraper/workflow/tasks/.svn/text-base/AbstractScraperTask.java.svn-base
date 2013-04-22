package nl.utwente.db.neogeo.scraper.workflow.tasks;

import java.lang.reflect.ParameterizedType;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.scraper.ScraperTask;
import nl.utwente.db.neogeo.scraper.ScraperWorkflow;
import nl.utwente.db.neogeo.scraper.messages.BaseScraperMessage;

import org.apache.log4j.Logger;

public abstract class AbstractScraperTask<InputType, OutputType> implements ScraperTask<InputType, OutputType> {
	protected ScraperWorkflow<?> scraperWorkflow;
	protected Iterator<ScraperMessage<InputType>> inputIterator;
	protected ScraperMessage<InputType> lastReceivedMessage;
	private Queue<ScraperMessage<InputType>> queue = new LinkedList<ScraperMessage<InputType>>();
	private Logger logger = Logger.getLogger(AbstractScraperTask.class);

	public ScraperWorkflow<?> getScraperWorkflow() {
		return scraperWorkflow;
	}

	public void setScraperWorkflow(ScraperWorkflow<?> scraperWorkflow) {
		this.scraperWorkflow = scraperWorkflow;
	}

	public Iterator<ScraperMessage<InputType>> getInputIterator() {
		return inputIterator;
	}

	public void setInputIterator(Iterator<ScraperMessage<InputType>> inputIterator) {
		this.inputIterator = inputIterator;
	}

	public boolean hasNext() {
		return !queue.isEmpty() || this.inputIterator.hasNext();
	}

	public ScraperMessage<InputType> getNextInputMessage() {
		if (!queue.isEmpty()) {
			lastReceivedMessage = queue.poll();
		} else {
			if (!this.inputIterator.hasNext()) {
				throw new ScraperException("No more input messages for " + this.getClass().getSimpleName() + ". Abort scraping.");
			}

			lastReceivedMessage = this.inputIterator.next();
		}
		
		logger.debug(this.getClass() + ": Incoming message: " + lastReceivedMessage);

		return lastReceivedMessage;
	}

	public void remove() {
		throw new RuntimeException("Not supported, only forward movement.");
	}

	@SuppressWarnings("unchecked")
	public ScraperMessage<OutputType> createScraperMessage(OutputType body) {
		ScraperMessage<OutputType> result = null;

		if (lastReceivedMessage == null) {
			result = new BaseScraperMessage<OutputType>(body);
		} else {
			result = (ScraperMessage<OutputType>)lastReceivedMessage.clone(body);
		}

		return result;
	}

	/**
	 * Only needs to be called when this object is not the input iterator
	 * @param scraperTask
	 * @param scraperMessage
	 */
	protected void sendMessage(Class<? extends ScraperTask<? extends Object, ? extends Object>> scraperTaskClass, ScraperMessage<? extends Object> scraperMessage) {
		ScraperTask<? extends Object, ? extends Object> scraperTask = this.getScraperWorkflow().getTask(scraperTaskClass);
		
		if (scraperTask == null) {
			throw new ScraperException("Unable to find scraperTask " + scraperTaskClass.getSimpleName() + " in workflow " + this.getScraperWorkflow() + " while sending message " + scraperMessage);
		}
		
		scraperTask.addMessage(scraperMessage);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void addMessage(ScraperMessage scraperMessage) {
		if (!getInputClass().isInstance(scraperMessage.getBody())) {
			throw new ScraperException("Invalid message entered into " + this.getClass().getSimpleName() + ": " + scraperMessage);
		}
		
		queue.add(scraperMessage);
	}

	@SuppressWarnings("unchecked")
	public Class<InputType> getInputClass() {
		return (Class<InputType>) ((ParameterizedType) getClass()
				.getGenericSuperclass()).getActualTypeArguments()[0];
	}
	
	@SuppressWarnings("unchecked")
	public Class<OutputType> getOutputClass() {
		return (Class<OutputType>) ((ParameterizedType) getClass()
				.getGenericSuperclass()).getActualTypeArguments()[1];
	}
}