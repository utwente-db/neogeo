package nl.utwente.db.neogeo.scraper.messages;

import java.util.Map.Entry;

import nl.utwente.db.neogeo.scraper.ScraperMessage;
import nl.utwente.db.neogeo.utils.BasePropertyContainer;

public class BaseScraperMessage<BodyType> extends BasePropertyContainer implements ScraperMessage<BodyType> {
	protected BodyType body;
	
	public BaseScraperMessage(BodyType body) {
		this.body = body;
	}

	public BodyType getBody() {
		return this.body;
	}
	
	public void setBody(BodyType body) {
		this.body = body;
	}
	
	public ScraperMessage<? extends Object> clone(Object newBody) {
		BaseScraperMessage<Object> newScraperMessage = new BaseScraperMessage<Object>(newBody);

		for (Entry<String, Object> entry : this.propertiesByName.entrySet()) {
			newScraperMessage.setProperty(entry.getKey(), entry.getValue());
		}
		
		for (Entry<Class<? extends Object>, Object> entry : this.propertiesByClass.entrySet()) {
			newScraperMessage.setProperty(entry.getKey(), entry.getValue());
		}
		
		return newScraperMessage;
	}

	@Override
	public String toString() {
		return "BaseScraperMessage [body=" + body + ", propertiesByName=" + propertiesByName
				+ ", propertiesByClass=" + propertiesByClass + "]";
	}
}
