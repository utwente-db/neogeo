package nl.utwente.db.neogeo.social.server.loggedin;

import nl.utwente.db.neogeo.social.server.NeoGeoSocialServlet;


public abstract class LoggedInServlet<LoggedInRequestClass extends LoggedInRequest, LoggedInResponseClass extends LoggedInResponse> extends NeoGeoSocialServlet<LoggedInRequestClass, LoggedInResponseClass> {
	private static final long serialVersionUID = 1L;

	@Override
	protected abstract LoggedInRequestClass createEmptyRequest();
	@Override
	protected abstract LoggedInResponseClass createEmptyResponse();
}