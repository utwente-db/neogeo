package nl.utwente.db.neogeo.server.admin.servlets.monitoredthread;

import nl.utwente.db.neogeo.server.InvalidRequestException;
import nl.utwente.db.neogeo.server.admin.servlets.NeoGeoAdminRequest;

public class MonitoredThreadRequest extends NeoGeoAdminRequest {
	private String id;
	private Long longId;
	
	public MonitoredThreadRequest() {
		requiredParameters.add("id");
	}
	
	@Override
	public void validate() throws InvalidRequestException {
		super.validate();

		try {
			setLongId(Long.parseLong(id));
		} catch (NumberFormatException e) {
			throw new InvalidRequestException("Invalid request. Check your id.", e);
		}
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public Long getLongId() {
		return longId;
	}

	public void setLongId(Long longId) {
		this.longId = longId;
	}
}

