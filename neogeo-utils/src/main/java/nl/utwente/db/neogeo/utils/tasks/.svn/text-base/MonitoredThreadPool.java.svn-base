package nl.utwente.db.neogeo.utils.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MonitoredThreadPool implements Iterable<MonitoredThread<?>> {
	protected Map<Long, MonitoredThread<?>> monitoredThreads = new HashMap<Long, MonitoredThread<?>>();
	private static MonitoredThreadPool instance;
	
	/**
	 * Use MonitoredThreadPool.getInstance() instead.
	 */
	protected MonitoredThreadPool() {}
	
	public MonitoredThread<?> getMonitoredThread(Long id) {
		MonitoredThread<?> result = monitoredThreads.get(id);
		
		if (result == null) {
			return result;
		}
		
		if (result.getProgress() >= 1 || !result.isAlive()) {
			monitoredThreads.remove(id);
			result.interrupt();
		}
		
		return result;
	}
	
	public void cleanup() {
		List<Long> idsToRemove = new ArrayList<Long>();
		
		for (MonitoredThread<?> thread : this) {
			if (!thread.isAlive() && (!thread.hasUpdates() || thread.getLastUpdateAccess() < System.currentTimeMillis() - 60000)) {
				// Thread has (been) terminated and there are no updates, or no process is monitoring them
				idsToRemove.add(thread.getId());
			}
		}
		
		for (Long idToRemove : idsToRemove) {
			monitoredThreads.remove(idToRemove);
		}
	}
	
	public boolean hasThread(Long id) {
		return monitoredThreads.containsKey(id);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public MonitoredThread<?> createMonitoredThread(MonitoredRunnable<?> runnable, String name) {
		MonitoredThread<?> result = new MonitoredThread(runnable, name);
		
		registerThread(result);
		
		return result;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public MonitoredThread<?> createMonitoredThread(MonitoredRunnable<?> runnable) {
		MonitoredThread<?> result = new MonitoredThread(runnable);
		
		registerThread(result);
		
		return result;
	}
	
	public void stopThread(Long id) {
		stopThread(getMonitoredThread(id));
	}
	
	public void stopThread(MonitoredThread<?> monitoredThread) {
		monitoredThreads.remove(monitoredThread.getId());
		monitoredThread.interrupt();
	}

	public void registerThread(MonitoredThread<?> thread) {
		monitoredThreads.put(thread.getId(), thread);
	}

	public static MonitoredThreadPool getInstance() {
		if (instance == null) {
			instance = new MonitoredThreadPool();
		}
		
		instance.cleanup();
		
		return instance;
	}

	public static void setInstance(MonitoredThreadPool instance) {
		MonitoredThreadPool.instance = instance;
	}

	public Iterator<MonitoredThread<?>> iterator() {
		return monitoredThreads.values().iterator();
	}

}
