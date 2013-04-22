package nl.utwente.db.neogeo.scraper.admin.servlets;

import java.util.List;

import nl.utwente.db.neogeo.server.admin.servlets.monitoredthread.start.MonitoredThreadStartRequest;

public class ScraperStartRequest extends MonitoredThreadStartRequest {
	private String workflow;
	private List<String> townNames;

	public ScraperStartRequest() {
		requiredParameters.add("workflow");
	}
	
	public String getWorkflow() {
		return workflow;
	}

	public void setWorkflow(String workflow) {
		this.workflow = workflow;
	}

	public List<String> getTownNames() {
		return townNames;
	}

	public void setTownNames(List<String> townNames) {
		this.townNames = townNames;
	}
}
