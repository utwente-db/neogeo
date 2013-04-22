package nl.utwente.db.neogeo.social.server.friends;

import java.util.List;

import nl.utwente.db.neogeo.social.server.NeoGeoSocialResponse;
import nl.utwente.db.neogeo.social.server.model.User;

public class FriendsResponse extends NeoGeoSocialResponse {
	private List<User> friends;

	public List<User> getFriends() {
		return friends;
	}

	public void setFriends(List<User> friends) {
		this.friends = friends;
	}
	
	// TODO this is a temporary solution
	@Override
	public String toString() {
		return friends.toString();
	}
}
