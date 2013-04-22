package nl.utwente.db.neogeo.scraper;

import nl.utwente.db.neogeo.utils.PropertyContainer;

public interface ScraperMessage<BodyType> extends PropertyContainer {
	public BodyType getBody();
	public void setBody(BodyType body);
	
	public ScraperMessage<? extends Object> clone(Object newBody);
}
