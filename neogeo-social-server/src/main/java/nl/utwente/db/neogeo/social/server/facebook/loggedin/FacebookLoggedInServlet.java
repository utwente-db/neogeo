package nl.utwente.db.neogeo.social.server.facebook.loggedin;

import nl.utwente.db.neogeo.social.server.facebook.FacebookConnection;
import nl.utwente.db.neogeo.social.server.loggedin.LoggedInServlet;

public class FacebookLoggedInServlet extends LoggedInServlet<FacebookLoggedInRequest, FacebookLoggedInResponse> {

	private static final long serialVersionUID = 1L;
	
	@Override
	public void handleRequest(FacebookLoggedInRequest request, FacebookLoggedInResponse response) {
		String accessToken = request.getAccessToken();
		FacebookConnection connection = new FacebookConnection(accessToken);
		
		String text = connection.handleLoggedInRequest(request);
		response.setText(text);
	}
	
	@Override
	protected FacebookLoggedInRequest createEmptyRequest() {
		return new FacebookLoggedInRequest();
	}
	
	@Override
	protected FacebookLoggedInResponse createEmptyResponse() {
		return new FacebookLoggedInResponse();
	}
}