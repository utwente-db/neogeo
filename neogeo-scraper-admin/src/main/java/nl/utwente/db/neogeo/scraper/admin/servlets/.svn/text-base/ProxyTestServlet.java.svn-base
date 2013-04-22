package nl.utwente.db.neogeo.scraper.admin.servlets;

import nl.utwente.db.neogeo.scraper.test.proxy.ProxyTestRunnable;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.MonitoredThreadResponse;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.start.MonitoredThreadStartRequest;
import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.start.MonitoredThreadStartServlet;
import nl.utwente.db.neogeo.utils.tasks.MonitoredRunnable;

public class ProxyTestServlet extends MonitoredThreadStartServlet<MonitoredThreadStartRequest, MonitoredThreadResponse> {
	private static final long serialVersionUID = 1L;
	private String testAddress = "http://www.detelefoongids.nl/bg-l/180727380030-Bakkerij+Meinders+Echte+Bakker/vermelding/";
	
	public String getTestAddress() {
		return testAddress;
	}

	public void setTestAddress(String testAddress) {
		this.testAddress = testAddress;
	}

	@Override
	public MonitoredRunnable<?> createRunnable(MonitoredThreadStartRequest request) {
		return new ProxyTestRunnable(testAddress);
	}
}
