package nl.utwente.db.neogeo.social.server.facebook;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.social.server.friends.FriendsRequest;
import nl.utwente.db.neogeo.social.server.loggedin.LoggedInRequest;
import nl.utwente.db.neogeo.social.server.model.Interest;
import nl.utwente.db.neogeo.social.server.model.SocialMediumConnection;
import nl.utwente.db.neogeo.social.server.model.User;
import nl.utwente.db.neogeo.utils.FileUtils;
import nl.utwente.db.neogeo.utils.SpringUtils;
import nl.utwente.db.neogeo.utils.WebUtils;
import nl.utwente.db.neogeo.utils.parallel.ParallelProcessor;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class FacebookConnection implements SocialMediumConnection {
	public final static String NAME = "Facebook";
	
	private String baseURL = "https://graph.facebook.com/";

	public static final String CURRENT_USER_ID = "me";
	public static final String FACEBOOK_ACCESS_TOKEN_PARAMETER = "access_token";

	public static final String USER_INFORMATION_PATH = "";
	public static final String FRIENDS_PATH = "/friends";
	public static final String NEWS_FEED_PATH = "/home";
	public static final String PROFILE_FEED_PATH = "/feed";
	public static final String LIKES_PATH = "/likes";
	public static final String MOVIES_PATH = "/movies";
	public static final String MUSIC_PATH = "/music";
	public static final String BOOKS_PATH = "/books";
	public static final String NOTES_PATH = "/notes";
	public static final String PERMISSIONS_PATH = "/permissions";
	public static final String PHOTOS_PATH = "/photos";
	public static final String ALBUMS_PATH = "/albums";
	public static final String VIDEOS_PATH = "/videos";
	public static final String VIDEOS_UPLOADED_PATH = "/videos/uploaded";
	public static final String EVENTS_PATH = "/events";
	public static final String GROUPS_PATH = "/groups";
	public static final String CHECKINS_PATH = "/checkins";
	
	public static final List<String> AVAILABLE_FRIEND_CONNECTION_PATHS = new ArrayList<String>();
	
	public static final List<String> AVAILABLE_USER_CONNECTION_PATHS = new ArrayList<String>();
	
	static {
		AVAILABLE_FRIEND_CONNECTION_PATHS.addAll(Arrays.asList(new String[]{
				LIKES_PATH,
				MOVIES_PATH,
				MUSIC_PATH,
				BOOKS_PATH,
				NOTES_PATH,
				PERMISSIONS_PATH,
				PHOTOS_PATH,
				ALBUMS_PATH,
				VIDEOS_PATH,
				VIDEOS_UPLOADED_PATH,
				EVENTS_PATH,
				GROUPS_PATH,
				CHECKINS_PATH
			}));
		
		AVAILABLE_USER_CONNECTION_PATHS.addAll(Arrays.asList(new String[]{
				FRIENDS_PATH,
				NEWS_FEED_PATH,
				PROFILE_FEED_PATH,
			}));
		
		AVAILABLE_USER_CONNECTION_PATHS.addAll(AVAILABLE_FRIEND_CONNECTION_PATHS);
	}
	
	public static void main(String[] args) {
		FacebookConnection connection = new FacebookConnection("AAACEdEose0cBAMYosNuQgnCMBhgO4meNR3qH1cjXNhenSu6GRD6HiTa20OCYdbn297Lf2pZALjodgovOYrfZB0K5KvL0ONYoSHKuOldxmnwN01tZBh8");
		connection.getCopyOfData();
		
		System.out.println("Done");
	}

	private final String accessToken;

	public FacebookConnection (String accessToken) {
		this.accessToken = accessToken;
	}
	
	public String handleLoggedInRequest(LoggedInRequest request) {
		// TODO store
	    User currentUser = getCurrentUser();
	    
		// TODO store
	    List<User> friends = getFriends(currentUser);
	    
	    return "Login to Facebook was processed";
	}
	
	public Set<User> handleFriendsRequest(FriendsRequest request) {
		// TODO Auto-generated method stub
		return null;
	}

	public User getCurrentUser() {
		return getUser(CURRENT_USER_ID);
	}
	
	@SuppressWarnings("unchecked")
	public User getUser(String externalId) {
		JSONObject response = getJSONData(buildUserURL(externalId));

		return createUser(response);
	}
	
	@SuppressWarnings("unchecked")
	public Interest getInterest(String externalId) {
		JSONObject response = getJSONData(buildGraphUrl(externalId));
		
		return createInterest(response);
	}
	
	public User completeUserInformation(FacebookUser user) {
		return getUser(user.getExternalId());
	}
	
	public List<User> completeUserInformation(List<User> users) {
		Object[] parameters = new Object[]{ParallelProcessor.INPUT_OBJECT};
	    ParallelProcessor<User, User> parallelProcessor = new ParallelProcessor<User, User>();
	    
	    try {
			return parallelProcessor.run(users, "completeUserInformation", parameters, this);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}
	
	public Interest completeInterestInformation(Interest interest) {
		return getInterest(interest.getExternalId());
	}
	
	public List<Interest> completeInterestInformation(List<Interest> interests) {
		Object[] parameters = new Object[]{ParallelProcessor.INPUT_OBJECT};
	    ParallelProcessor<Interest, Interest> parallelProcessor = new ParallelProcessor<Interest, Interest>();
	    
	    try {
			return parallelProcessor.run(interests, "completeInterestInformation", parameters, this);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected String buildUserURL(String externalId) {
		return buildGraphUrl(externalId);
	}
	
	protected String buildInterestURL() {
		return buildInterestURL(CURRENT_USER_ID);
	}
	
	protected String buildInterestURL(String externalId) {
		return buildGraphUrl(externalId + LIKES_PATH);
	}
	
	/**
	 * Returns ids and names only. Equivalent to getFriends(true)
	 * @param rootUserID
	 * @return
	 */
	public List<User> getCurrentUsersFriends() {
		return getCurrentUsersFriends(true);
	}
	
	/**
	 * Returns ids and names only. Equivalent to getFriends(CURRENT_USER_ID, idsAndNamesOnly)
	 * @param idsAndNamesOnly - Much faster when true.
	 * @return
	 */
	public List<User> getCurrentUsersFriends(boolean idsAndNamesOnly) {
		return getFriends(CURRENT_USER_ID, idsAndNamesOnly);
	}
	
	public List<User> getFriends(User user) {
		return getFriends(user.getExternalId());
	}

	public List<User> getFriends(User user, boolean idsAndNamesOnly) {
		return getFriends(user.getExternalId(), idsAndNamesOnly);
	}

	/**
	 * Returns ids and names only. Equivalent to getFriends(rootUserID, accessToken, true)
	 * @param rootUserID
	 * @return
	 */
	public List<User> getFriends(String externalId) {
		return getFriends(externalId, true);
	}
	
	/**
	 * 
	 * @param rootUserID
	 * @param idsAndNamesOnly - Much faster when true.
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<User> getFriends(String externalId, boolean idsAndNamesOnly) {
		// Temporary list of only IDs and names
		List<User> basicResult = new ArrayList<User>();
		List<User> result;

		String url = buildFriendsURL(externalId);
		
		JSONObject jsonFriendsContainer = WebUtils.jsonURLToSimpleJSONObject(url);
		JSONArray jsonFriends = (JSONArray)jsonFriendsContainer.get("data");
		
		for (Object jsonFriend : jsonFriends) {
			User friend = createUser((JSONObject)jsonFriend);
			basicResult.add(friend);
		}

		if (idsAndNamesOnly) {
			result = basicResult;
		} else {
			result = completeUserInformation(basicResult);
		}
		
		return result;
	}
	
	public FacebookUser createUser(Map<String, String> facebookData) {
		FacebookUser user = new FacebookUser();
		SpringUtils.fillFromMap(user, facebookData);
		
		user.setExternalId(user.getId());
		user.setId(getPrefix() + ":" + user.getId());
		
		return user;
	}

	public List<Interest> getCurrentUsersInterests() {
		return getInterests(CURRENT_USER_ID);
	}
	
	public List<Interest> getCurrentUsersInterests(boolean idsAndNamesOnly) {
		return getInterests(CURRENT_USER_ID, idsAndNamesOnly);
	}
	
	public List<Interest> getInterests(User user) {
		return getInterests(user.getExternalId());
	}
	
	public List<Interest> getInterests(User user, boolean idsAndNamesOnly) {
		return getInterests(user.getExternalId(), idsAndNamesOnly);
	}
	
	public List<Interest> getInterests(String externalId) {
		return getInterests(externalId, true);
	}
	
	@SuppressWarnings("unchecked")
	public List<Interest> getInterests(String externalId, boolean idsAndNamesOnly) {
		// Temporary list of only IDs and names
		List<Interest> basicResult = new ArrayList<Interest>();
		List<Interest> result;

		JSONObject response = getJSONData(buildInterestURL(externalId));
		JSONArray jsonInterests = (JSONArray)response.get("data");
		
		FileUtils.writeFile("src/main/resources/facebook/likes/" + externalId + ".json", jsonInterests.toJSONString());
		
		for (Object jsonInterest : jsonInterests) {
			Interest interest = createInterest((JSONObject)jsonInterest);
			basicResult.add(interest);
		}
		
		if (idsAndNamesOnly) {
			result = basicResult;
		} else {
			result = completeInterestInformation(basicResult);
		}

		return result;
	}

	protected Interest createInterest(Map<String, String> facebookData) {
		Interest interest = new Interest();
		SpringUtils.fillFromMap(interest, facebookData);
		
		interest.setExternalId(interest.getId());
		interest.setId(getPrefix() + ":" + interest.getId());
		
		return interest;
	}
		
	private JSONObject getJSONData(String facebookURL) {
		try {
			return WebUtils.jsonURLToSimpleJSONObject(facebookURL);
		} catch (NeoGeoException e) {
			throw new NeoGeoException("Unable to retrieve data from Facebook. Check your access token." + facebookURL, e);
		} catch (ClassCastException e) {
			throw new NeoGeoException("Unable to parse data from Facebook. URL: " + facebookURL, e);
		}
	}
	
	protected String buildFriendsURL(String externalId) {
		return buildGraphUrl(externalId + FRIENDS_PATH);
	}

	protected String buildGraphUrl(String path) {
		String parameterPrefix = "?";
		
		if (path.contains("?")) {
			parameterPrefix = "&";
		}
		
		return getBaseURL() + path + parameterPrefix + FACEBOOK_ACCESS_TOKEN_PARAMETER + "=" + accessToken;
	}
	
	public void getCopyOfData() {
		User currentUser = getCurrentUser();
		getCopyOfData((FacebookUser)currentUser, true);
		
		// possible optimalization: do this before / while getting copy of data of current user (e.g. by adding the user to the friends)
		List<User> friends = getCurrentUsersFriends();
		
		Object[] parameters = new Object[]{ParallelProcessor.INPUT_OBJECT, false};
	    ParallelProcessor<User, User> parallelProcessor = new ParallelProcessor<User, User>();
	    
	    try {
			parallelProcessor.run(friends, "getCopyOfData", parameters, this);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	public void getCopyOfData(FacebookUser user, Boolean currentUser) {
		getCopyOfData(user.getExternalId(), currentUser);
	}

	public void getCopyOfData(String externalId, Boolean currentUser) {
		// make sure this data is there first, then the other data
		getCopyOfData(USER_INFORMATION_PATH, externalId);
		getCopyOfConnectionData(externalId, currentUser);
	}
	
	private void getCopyOfConnectionData(String externalId, Boolean currentUser) {
		List<String> connectionPaths = currentUser ? AVAILABLE_USER_CONNECTION_PATHS : AVAILABLE_FRIEND_CONNECTION_PATHS;
		
		Object[] parameters = new Object[]{ParallelProcessor.INPUT_OBJECT, externalId};
	    ParallelProcessor<String, String> parallelProcessor = new ParallelProcessor<String, String>();
	    
	    try {
			parallelProcessor.run(connectionPaths, "getCopyOfData", parameters, this);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 
	 * @param itemName - For possibilities, see FacebookConnection.AVAILABLE_USER_CONNECTION_PATHS
	 * @param externalId
	 */
	public void getCopyOfData(String itemName, String externalId) {
		String url = buildGraphUrl(externalId + itemName);
		int count = 0;
		Set<String> visitedURLs = new HashSet<String>();
		
		while(true) {
			visitedURLs.add(url);
			String fileName = "src/main/resources/facebook" + itemName + "/" + externalId + "_" + count++ + ".json";
	
			String contents = WebUtils.getContent(url);
			// TODO write directly to DB instead
			FileUtils.writeFile(fileName, contents);
			
			JSONObject jsonObject = WebUtils.jsonURLToSimpleJSONObject(url);
			JSONObject paging = (JSONObject)jsonObject.get("paging");
			
			if (paging == null || !paging.containsKey("next")) {
				break;
			}
			
			url = ((String)paging.get("next")).replace("&limit=25&", "&limit=500&");
			
			if (visitedURLs.contains(url)) {
				break;
			}
		}
	}

	public static String getPrefix() {
		return "facebook";
	}

	public String getBaseURL() {
		return baseURL;
	}

	public void setBaseURL(String baseURL) {
		this.baseURL = baseURL;
	}
}