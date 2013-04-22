package nl.utwente.db.neogeo.scraper;

public class ScrapingNotAllowedException extends ScraperException {
	private static final long serialVersionUID = 1L;

	public ScrapingNotAllowedException(String message) {
		super(message);
	}
	
	public ScrapingNotAllowedException(String message, Throwable cause) {
		super(message, cause);
	}
}
