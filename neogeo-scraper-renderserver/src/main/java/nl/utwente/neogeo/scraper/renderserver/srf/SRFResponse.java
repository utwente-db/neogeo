package nl.utwente.neogeo.scraper.renderserver.srf;

import java.util.List;

import nl.utwente.db.neogeo.server.servlets.NeoGeoResponse;

public class SRFResponse extends NeoGeoResponse {
	private List<String> results;

	public List<String> getResults() {
		return results;
	}

	public void setResults(List<String> results) {
		this.results = results;
	}

}
