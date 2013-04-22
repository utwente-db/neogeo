package nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.stop;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.MonitoredThreadRequest;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.MonitoredThreadResponse;
import nl.utwente.db.neogeo.server.servlets.NeoGeoServlet;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThread;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThreadPool;

public class MonitoredThreadStopServlet extends NeoGeoServlet<MonitoredThreadRequest, MonitoredThreadResponse> {
	private static final long serialVersionUID = 1L;

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		super.doGet(request, response);
		
		redirect(response, request.getParameter("sourceURI"));
	}
	
	@Override
	public void handleRequest(MonitoredThreadRequest request, MonitoredThreadResponse response) {
		MonitoredThreadPool pool = MonitoredThreadPool.getInstance();
		MonitoredThread<?> thread = pool.getMonitoredThread(request.getLongId());
		
		// Stop the threads belonging to that group
		pool.stopThread(thread);
	}
}