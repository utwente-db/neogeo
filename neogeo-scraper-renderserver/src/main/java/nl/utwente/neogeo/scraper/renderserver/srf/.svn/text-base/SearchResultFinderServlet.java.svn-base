package nl.utwente.neogeo.scraper.renderserver.srf;

import java.util.Vector;

import nl.utwente.db.neogeo.scraper.srf.SearchResultFinder;
import nl.utwente.db.neogeo.server.servlets.NeoGeoServlet;

public class SearchResultFinderServlet extends NeoGeoServlet<SRFRequest, SRFResponse> {

	private static final long serialVersionUID = 1L;

	@Override
	public void handleRequest(SRFRequest request, SRFResponse response) {
		String url = request.getUrl();
		Vector<String> xPaths = SearchResultFinder.robustSRF(url);
		
		response.setResults(xPaths);
	}

}
