package nl.utwente.db.neogeo.social.server.facebook.friends;

import nl.utwente.db.neogeo.social.server.friends.FriendsRequest;

public class FacebookFriendsRequest extends FriendsRequest {
	private String accessToken;
	private boolean idsAndNamesOnly = true;
	
	public FacebookFriendsRequest() {
		super();
		
		requiredParameters.add("accessToken");
	}

	public String getAccessToken() {
		return accessToken;
	}

	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}

	public boolean isIdsAndNamesOnly() {
		return idsAndNamesOnly;
	}

	public void setIdsAndNamesOnly(boolean idsAndNamesOnly) {
		this.idsAndNamesOnly = idsAndNamesOnly;
	}
}