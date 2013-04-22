package nl.utwente.db.neogeo.server.admin.servlets.test;

import nl.utwente.db.neogeo.server.admin.DummyTestRunnable;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.MonitoredThreadResponse;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.start.MonitoredThreadStartRequest;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.start.MonitoredThreadStartServlet;
import nl.utwente.db.neogeo.utils.tasks.MonitoredRunnable;

public class DummyTestServlet extends MonitoredThreadStartServlet<MonitoredThreadStartRequest, MonitoredThreadResponse> {
	private static final long serialVersionUID = 1L;
	@Override
	public MonitoredRunnable<?> createRunnable(MonitoredThreadStartRequest request) {
		return new DummyTestRunnable();
	}
}
