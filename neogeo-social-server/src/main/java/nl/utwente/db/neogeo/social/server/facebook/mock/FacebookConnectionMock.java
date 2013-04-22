package nl.utwente.db.neogeo.social.server.facebook.mock;

import nl.utwente.db.neogeo.social.server.facebook.FacebookConnection;

public class FacebookConnectionMock extends FacebookConnection {
	
	public FacebookConnectionMock(String baseURL) {
		super((String)null);
		this.setBaseURL(baseURL);
	}
}
