package nl.utwente.db.neogeo.social.server.facebook.loggedin;

import nl.utwente.db.neogeo.social.server.loggedin.LoggedInResponse;

public class FacebookLoggedInResponse extends LoggedInResponse {
	private String text = "";

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
	
	public void addText(String text) {
		this.text += text;
	}
	
	public String toString() {
		return getText();
	}
}
