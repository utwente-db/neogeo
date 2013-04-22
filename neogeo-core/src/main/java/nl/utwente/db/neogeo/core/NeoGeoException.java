package nl.utwente.db.neogeo.core;

public class NeoGeoException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public NeoGeoException(String detailMessage) {
		super(detailMessage);
	}
	
	public NeoGeoException(String detailMessage, Throwable cause) {
		super(detailMessage, cause);
	}
	
	public NeoGeoException(Throwable cause) {
		super(cause);
	}
}