package nl.utwente.db.neogeo.server.admin.servlets;

import nl.utwente.db.neogeo.server.InvalidRequestException;
import nl.utwente.db.neogeo.server.servlets.NeoGeoRequest;

public class NeoGeoAdminRequest extends NeoGeoRequest {
	private String key;
	
	public NeoGeoAdminRequest() {
		requiredParameters.add("key");
	}
	
	@Override
	public void validate() throws InvalidRequestException {
		super.validate();

		// TODO improve the security of this :)
		if (!"secretKey".equals(getKey())) {
			throw new InvalidRequestException("Invalid request. Check your key.");
		}
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}





}
