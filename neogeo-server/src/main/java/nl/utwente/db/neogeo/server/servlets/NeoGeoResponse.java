package nl.utwente.db.neogeo.server.servlets;

import nl.utwente.db.neogeo.utils.WebUtils;

public class NeoGeoResponse {
	public final static String CONTENT_TYPE_JSON = "application/json";
	public final static String CONTENT_TYPE_HTML = "text/html";
	
	private String contentType = CONTENT_TYPE_JSON;
	private int responseCode = 200;
	private String text = "";
	private String sourceURL = "";

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	
	public int getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	public String toJson() {
		return WebUtils.javaToJSON(this, false);
	}
	
	public String toHTML() {
		return WebUtils.javaToHTML(this);
	}
	
	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
	
	public void addText(String text) {
		this.text += text;
	}

	public String getSourceURL() {
		return sourceURL;
	}

	public void setSourceURL(String sourceURL) {
		this.sourceURL = sourceURL;
	}

	@Override
	public String toString() {
		if (CONTENT_TYPE_JSON.equals(this.getContentType())) {
			return toJson();
		} else if (CONTENT_TYPE_HTML.equals(this.getContentType())) {
			return toHTML();
		} else {
			return getText();
		}
	}
}
