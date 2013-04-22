package nl.utwente.db.neogeo.server.admin.servlets.test;

import nl.utwente.db.neogeo.server.admin.ServerTest;
import nl.utwente.db.neogeo.utils.tasks.MonitoredRunnable;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThread;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThreadPool;
import nl.utwente.db.neogeo.utils.test.TestResult;

public abstract class AbstractServerTest implements ServerTest {
	@SuppressWarnings("unchecked")
	public MonitoredThread<?> test() {
		MonitoredThreadPool monitoredThreadPool = MonitoredThreadPool.getInstance();
		MonitoredThread<TestResult> result = (MonitoredThread<TestResult>)monitoredThreadPool.createMonitoredThread(createTestTask());
		
		result.start();
		return result;
	}

	protected abstract MonitoredRunnable<?> createTestTask();
}
