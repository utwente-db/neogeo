package nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.status;

import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.MonitoredThreadRequest;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.MonitoredThreadResponse;
import nl.utwente.db.neogeo.server.servlets.NeoGeoServlet;
import nl.utwente.db.neogeo.utils.StringUtils;
import nl.utwente.db.neogeo.utils.WebUtils;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThread;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThreadPool;

public class MonitoredThreadStatusServlet extends NeoGeoServlet<MonitoredThreadRequest, MonitoredThreadResponse> {
	private static final long serialVersionUID = 1L;
	public final static String TABLE_HEADERS_SENT_PROPERTY = MonitoredThreadStatusServlet.class.getName() + ".tableHeadersSent";
	
	@Override
	public void handleRequest(MonitoredThreadRequest request, MonitoredThreadResponse response) {
		String id = request.getId();
		long longId = Long.parseLong(id);
		
		MonitoredThreadPool monitoredThreadPool = MonitoredThreadPool.getInstance();
		MonitoredThread<?> monitoredThread = monitoredThreadPool.getMonitoredThread(longId);
		
		if (monitoredThread == null) {
			// Thread has been completed and no more updates are available
			return;
		}
		
		boolean tableHeadersSent = monitoredThread.getProperty(TABLE_HEADERS_SENT_PROPERTY) != null;
		
		String text = WebUtils.collectionToHTML(monitoredThread.getUpdates(), !tableHeadersSent);
		
		tableHeadersSent |= !StringUtils.isEmpty(text);
		monitoredThread.setProperty(TABLE_HEADERS_SENT_PROPERTY, tableHeadersSent);
		
		response.read(monitoredThread);
		response.setText(text);
	}
}