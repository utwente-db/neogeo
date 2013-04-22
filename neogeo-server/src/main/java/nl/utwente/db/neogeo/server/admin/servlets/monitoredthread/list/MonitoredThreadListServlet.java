package nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.list;

import java.util.ArrayList;
import java.util.List;

import nl.utwente.db.neogeo.server.admin.servlets.NeoGeoAdminRequest;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.MonitoredThreadResponse;
import nl.utwente.db.neogeo.server.servlets.NeoGeoResponse;
import nl.utwente.db.neogeo.server.servlets.NeoGeoServlet;
import nl.utwente.db.neogeo.utils.WebUtils;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThread;
import nl.utwente.db.neogeo.utils.tasks.MonitoredThreadPool;

public class MonitoredThreadListServlet extends NeoGeoServlet<NeoGeoAdminRequest, NeoGeoResponse> {
	private static final long serialVersionUID = 1L;

	@Override
	public void handleRequest(NeoGeoAdminRequest request, NeoGeoResponse response) {
		MonitoredThreadPool pool = MonitoredThreadPool.getInstance();
		List<MonitoredThreadResponse> serverThreads = new ArrayList<MonitoredThreadResponse>();
		
		for (MonitoredThread<?> thread : pool) {
			MonitoredThreadResponse monitoredThreadResponse = new MonitoredThreadResponse();
			monitoredThreadResponse.read(thread);
			
			serverThreads.add(monitoredThreadResponse);
		}
		
		String responseText = WebUtils.collectionToHTML(serverThreads, true, MonitoredThreadResponse.class);
		response.setText(responseText);
	}
}
