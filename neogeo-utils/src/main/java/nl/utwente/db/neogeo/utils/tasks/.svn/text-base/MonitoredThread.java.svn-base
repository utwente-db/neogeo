package nl.utwente.db.neogeo.utils.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import nl.utwente.db.neogeo.utils.BasePropertyContainer;
import nl.utwente.db.neogeo.utils.PropertyContainer;

import org.apache.log4j.Logger;

public class MonitoredThread<OutputType> extends Thread implements PropertyContainer {
	protected PropertyContainer propertyContainer = new BasePropertyContainer();
	protected final MonitoredRunnable<OutputType> runnable;
	protected long lastUpdateAccess = System.currentTimeMillis();
	
	private Logger logger = Logger.getLogger(MonitoredThread.class);
	
	/**
	 * Package access only, use MonitoredTaskPool.getInstance().createMonitoredTask() instead
	 */
	MonitoredThread(MonitoredRunnable<OutputType> runnable) {
		this(runnable, runnable.getClass().getSimpleName().replace("Runnable", ""));
	}
	
	/**
	 * Package access only, use MonitoredTaskPool.getInstance().createMonitoredTask() instead
	 */
	MonitoredThread(MonitoredRunnable<OutputType> runnable, String name) {
		super(runnable, name);
		this.runnable = runnable;
	}
	
	public List<OutputType> getUpdates() {
		List<OutputType> result = new ArrayList<OutputType>();
		Queue<OutputType> updateQueue = runnable.getMessageQueue();
		
		while (!updateQueue.isEmpty()) {
			result.add(updateQueue.poll());
		}
		
		logger.debug("Sending updates " + result);
		lastUpdateAccess = System.currentTimeMillis();
		
		return result;
	}
	
	public double getProgress() {
		return runnable.getProgress();
	}
	
	public void setProperty(String propertyName, Object propertyValue) {
		propertyContainer.setProperty(propertyName, propertyValue);
	}

	public void setProperty(Class<? extends Object> propertyClass,
			Object propertyValue) {
		propertyContainer.setProperty(propertyClass, propertyValue);
	}

	public Object getProperty(String propertyName) {
		return propertyContainer.getProperty(propertyName);
	}

	public Object getProperty(Class<? extends Object> clazz) {
		return propertyContainer.getProperty(clazz);
	}

	public String getStringProperty(String propertyName) {
		return propertyContainer.getStringProperty(propertyName);
	}

	public void interrupt() {
		this.runnable.stop();
	}

	public boolean hasUpdates() {
		return !runnable.getMessageQueue().isEmpty();
	}
	
	@Override
	public String toString() {
		return "MonitoredThread [propertyContainer=" + propertyContainer
				+ ", runnable=" + runnable + ", lastUpdateAccess="
				+ lastUpdateAccess + ", logger=" + logger + "]";
	}

	public long getLastUpdateAccess() {
		return lastUpdateAccess;
	}

	public void setLastUpdateAccess(long lastUpdateAccess) {
		this.lastUpdateAccess = lastUpdateAccess;
	}

	public Queue<OutputType> getMessageQueue() {
		return runnable.getMessageQueue();
	}
}
