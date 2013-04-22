package nl.utwente.db.neogeo.server.utils;

import javax.servlet.http.HttpServletRequest;

public class ServerUtils {

	public static String getReferrer(HttpServletRequest request) {
		return request.getHeader("referer");
	}

	public static String getRequestURL(HttpServletRequest request) {
	    String requestUrl = request.getRequestURL().toString();
	    String queryString = request.getQueryString();   // d=789
	    
	    if (queryString != null) {
	    	requestUrl += "?" + queryString;
	    }
	    
	    return requestUrl;
	}

}
