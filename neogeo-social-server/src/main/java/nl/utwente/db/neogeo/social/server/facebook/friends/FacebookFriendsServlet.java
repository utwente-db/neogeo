package nl.utwente.db.neogeo.social.server.facebook.friends;

import java.util.List;

import nl.utwente.db.neogeo.social.server.facebook.FacebookConnection;
import nl.utwente.db.neogeo.social.server.friends.FriendsServlet;
import nl.utwente.db.neogeo.social.server.model.User;

public class FacebookFriendsServlet extends FriendsServlet<FacebookFriendsRequest, FacebookFriendsResponse> {
	private static final long serialVersionUID = 1L;

	public FacebookFriendsRequest createNewRequest() {
		return new FacebookFriendsRequest();
	}

	@Override
	public void handleRequest(FacebookFriendsRequest request, FacebookFriendsResponse response) {
		String accessToken = request.getAccessToken();
		FacebookConnection connection = new FacebookConnection(accessToken);
		
		List<User> friends = connection.getCurrentUsersFriends(request.isIdsAndNamesOnly());
		response.setFriends(friends);
	}

	@Override
	protected FacebookFriendsRequest createEmptyRequest() {
		return new FacebookFriendsRequest();
	}

	@Override
	protected FacebookFriendsResponse createEmptyResponse() {
		return new FacebookFriendsResponse();
	}
}
