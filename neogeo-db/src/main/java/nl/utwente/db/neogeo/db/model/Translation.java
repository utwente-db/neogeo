package nl.utwente.db.neogeo.db.model;

import nl.utwente.db.neogeo.core.model.BaseModelObject;

public class Translation extends BaseModelObject {
	private String fromLanguageCode;
	private String fromLanguageText;
	private String toLanguageCode;
	private String toLanguageText;
	
	public String getFromLanguageCode() {
		return fromLanguageCode;
	}
	
	public void setFromLanguageCode(String fromLanguageCode) {
		this.fromLanguageCode = fromLanguageCode;
		this.setName(this.fromLanguageCode + ": " + this.fromLanguageText);
	}
	
	public String getFromLanguageText() {
		return fromLanguageText;
	}
	
	public void setFromLanguageText(String fromLanguageText) {
		this.fromLanguageText = fromLanguageText;
		this.setName(this.fromLanguageCode + ": " + this.fromLanguageText);
	}
	
	public String getToLanguageCode() {
		return toLanguageCode;
	}
	
	public void setToLanguageCode(String toLanguageCode) {
		this.toLanguageCode = toLanguageCode;
	}
	
	public String getToLanguageText() {
		return toLanguageText;
	}
	
	public void setToLanguageText(String toLanguageText) {
		this.toLanguageText = toLanguageText;
	}
}
