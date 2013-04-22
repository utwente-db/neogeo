package nl.utwente.db.neogeo.social.server.welcomeservlet;

import nl.utwente.db.neogeo.server.servlets.NeoGeoResponse;
import nl.utwente.db.neogeo.server.servlets.welcome.WelcomeRequest;
import nl.utwente.db.neogeo.server.servlets.welcome.WelcomeResponse;

public class WelcomeServlet extends nl.utwente.db.neogeo.server.servlets.welcome.WelcomeServlet {
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

