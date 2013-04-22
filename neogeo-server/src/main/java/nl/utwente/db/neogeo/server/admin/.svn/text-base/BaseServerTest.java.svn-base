package nl.utwente.db.neogeo.server.admin;

import nl.utwente.db.neogeo.utils.tasks.MonitoredRunnable;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThread;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThreadPool;
import nl.utwente.db.neogeo.utils.test.TestResult;

public class BaseServerTest implements ServerTest {
	protected final MonitoredRunnable<?> task;
	
	public BaseServerTest(MonitoredRunnable<?> task) {
		this.task = task;
	}
	
	@SuppressWarnings("unchecked")
	public MonitoredThread<?> test() {
		MonitoredThreadPool monitoredThreadPool = MonitoredThreadPool.getInstance();
		MonitoredThread<TestResult> result = (MonitoredThread<TestResult>)monitoredThreadPool.createMonitoredThread(task);
		
		result.start();
		return result;
	}
}
