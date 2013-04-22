package nl.utwente.neogeo.scraper.renderserver.srf;

import nl.utwente.db.neogeo.server.admin.servlets.NeoGeoAdminRequest;

public class SRFRequest extends NeoGeoAdminRequest {
	private String url;
	
	public SRFRequest() {
		requiredParameters.add("url");
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
}
