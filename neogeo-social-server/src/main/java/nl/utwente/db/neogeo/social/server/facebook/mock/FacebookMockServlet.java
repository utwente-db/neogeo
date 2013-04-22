package nl.utwente.db.neogeo.social.server.facebook.mock;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.utwente.db.neogeo.social.server.NeoGeoSocialResponse;
import nl.utwente.db.neogeo.utils.FileUtils;

public class FacebookMockServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
	    PrintWriter out = response.getWriter();
	    
	    String uri = request.getRequestURI();
	    String pathToServlet = request.getContextPath() + "/facebook-mock/";
	    
	    String requestPath = uri.substring(pathToServlet.length());
	    String userId, folderName;
	    
	    if (requestPath.contains("/")) {
	    	userId = requestPath.substring(0, requestPath.indexOf("/"));
	    	folderName = requestPath.substring(requestPath.indexOf("/"));
	    } else {
	    	userId = requestPath;
	    	folderName = "";
	    }
	    
	    String fileName = "facebook" + folderName + "/" + userId + ".json";
	    
	    response.setContentType(NeoGeoSocialResponse.CONTENT_TYPE_JSON);
	    out.write(FileUtils.getFileAsString(fileName));
	}

}
