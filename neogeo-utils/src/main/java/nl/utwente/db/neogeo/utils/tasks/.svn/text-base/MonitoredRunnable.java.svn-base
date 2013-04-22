package nl.utwente.db.neogeo.utils.tasks;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class MonitoredRunnable<OutputType> implements Runnable {
	
	private Queue<OutputType> messageQueue = new ConcurrentLinkedQueue<OutputType>();

	public abstract void run();
	public abstract void stop();
	public abstract double getProgress();
	
	@SuppressWarnings("unchecked")
	public MonitoredThread<OutputType> getMonitoredThread() {
		Thread currentThread = Thread.currentThread();
		
		if (currentThread instanceof MonitoredThread<?>) {
			return (MonitoredThread<OutputType>)currentThread;
		} else {
			throw new RuntimeException("Running a MonitoredRunnable outside a MonitoredThread");
		}
	}
	
	public void addUpdate(OutputType update) {
		this.messageQueue.add(update);
	}

	public Queue<OutputType> getMessageQueue() {
		return messageQueue;
	}

	public void setMessageQueue(Queue<OutputType> messageQueue) {
		this.messageQueue = messageQueue;
	}
}