package nl.utwente.db.neogeo.server.servlets.welcome;

import nl.utwente.db.neogeo.server.servlets.NeoGeoResponse;
import nl.utwente.db.neogeo.server.servlets.NeoGeoServlet;

public class WelcomeServlet extends NeoGeoServlet<WelcomeRequest, WelcomeResponse> {
	private static final long serialVersionUID = 1L;

	@Override
	public void handleRequest(WelcomeRequest request, WelcomeResponse response) {
		response.setContentType(NeoGeoResponse.CONTENT_TYPE_HTML);
		
		response.addText("Welcome to NeoGeo Social Server<br/>");
		response.addText("<a href=\"friends\">Friends</a><br/>");
		response.addText("<a href=\"friends/facebook\">Facebook Friends</a><br/>");
		response.addText("<a href=\"loggedin/facebook\">Facebook Logged In</a><br/>");
	}
}