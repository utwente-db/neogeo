package nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.start;

import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.MonitoredThreadResponse;
import nl.utwente.db.neogeo.server.servlets.NeoGeoServlet;
import nl.utwente.db.neogeo.utils.tasks.MonitoredRunnable;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThread;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThreadPool;

import org.apache.log4j.Logger;

public abstract class MonitoredThreadStartServlet<RequestClass extends MonitoredThreadStartRequest, ResponseClass extends MonitoredThreadResponse> extends NeoGeoServlet<RequestClass, ResponseClass> {
	private static final long serialVersionUID = 1L;
	Logger logger = Logger.getLogger(MonitoredThreadStartServlet.class);

	@Override
	public void handleRequest(RequestClass request, ResponseClass response) {
		MonitoredRunnable<?> runnable = createRunnable(request);
		MonitoredThread<?> monitoredThread = MonitoredThreadPool.getInstance().createMonitoredThread(runnable);
		
		monitoredThread.start();

		response.read(monitoredThread);
	}

	public abstract MonitoredRunnable<?> createRunnable(RequestClass request);
}
