package nl.utwente.db.neogeo.social.server.model;

import java.util.List;
import java.util.Set;

import nl.utwente.db.neogeo.social.server.friends.FriendsRequest;
import nl.utwente.db.neogeo.social.server.loggedin.LoggedInRequest;

public interface SocialMediumConnection {

	public String handleLoggedInRequest(LoggedInRequest request);
	public Set<User> handleFriendsRequest(FriendsRequest request);
	
	public List<User> getCurrentUsersFriends();
	public List<User> getCurrentUsersFriends(boolean idsAndNamesOnly);
	public List<User> getFriends(User user);
	public List<User> getFriends(User user, boolean idsAndNamesOnly);
	public List<User> getFriends(String externalId);
	public List<User> getFriends(String externalId, boolean idsAndNamesOnly);

	public List<Interest> getCurrentUsersInterests();
	public List<Interest> getCurrentUsersInterests(boolean idsAndNamesOnly);
	public List<Interest> getInterests(User user);
	public List<Interest> getInterests(User user, boolean idsAndNamesOnly);
	public List<Interest> getInterests(String externalId);
	public List<Interest> getInterests(String externalId, boolean idsAndNamesOnly);
}
