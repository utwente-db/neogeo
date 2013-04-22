package nl.utwente.db.neogeo.server.servlets;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.ParameterizedType;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.server.utils.ServerUtils;

import org.apache.log4j.Logger;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

public abstract class NeoGeoServlet<RequestClass extends NeoGeoRequest, ResponseClass extends NeoGeoResponse> extends HttpServlet {
	private static final long serialVersionUID = 1L;
	Logger logger = Logger.getLogger(NeoGeoServlet.class);
	
	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
	    PrintWriter out = response.getWriter();
	    
	    RequestClass neoGeoRequest = createEmptyRequest();
	    ResponseClass neoGeoResponse = createEmptyResponse();
	    
	    try {
	    	neoGeoRequest.initFromServletRequest(request);
	    } catch (NeoGeoException e) {
	    	response.setStatus(400);
	    	out.write(e.getMessage());
	    	
	    	return;
	    }
	    
	    try {
	    	handleRequest(neoGeoRequest, neoGeoResponse);

	    	String newSourceURL = ServerUtils.getRequestURL(request).replace("sourceURL=" + request.getParameter("sourceURL"), "");
	    	neoGeoResponse.setSourceURL(newSourceURL);
	    	
	    	response.setContentType(neoGeoResponse.getContentType());
	    	out.write(neoGeoResponse.toString());
	    } catch (NeoGeoException e) {
	    	response.setStatus(400);
	    	out.write("Unable to finish handling your request: " + e.getMessage());
		}
	}
	
	/**
	 * Default behavior for a POST is the same as for a GET in NeoGeo
	 */
	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
		doGet(request, response);
	}
		
	public abstract void handleRequest(RequestClass request, ResponseClass response);
	
	@SuppressWarnings("unchecked")
	protected RequestClass createEmptyRequest() {
		Class<RequestClass> requestClass = getRequestClass();
		BeanWrapper beanWrapper = new BeanWrapperImpl(requestClass);
		
		return (RequestClass)beanWrapper.getWrappedInstance();
	}

	@SuppressWarnings("unchecked")
	protected ResponseClass createEmptyResponse() {
		Class<ResponseClass> responseClass = getResponseClass();
		BeanWrapper beanWrapper = new BeanWrapperImpl(responseClass);
		
		return (ResponseClass)beanWrapper.getWrappedInstance();
	}
	
	@SuppressWarnings("unchecked")
	protected Class<RequestClass> getRequestClass() {
		return (Class<RequestClass>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	}
	
	@SuppressWarnings("unchecked")
	protected Class<ResponseClass> getResponseClass() {
		return (Class<ResponseClass>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[1];
	}

	protected void redirect(HttpServletResponse response, String redirectURL) throws IOException {
		String encodedURL = response.encodeRedirectURL(redirectURL);
		response.sendRedirect(encodedURL);
	}
	 
	protected void forward(HttpServletRequest request, HttpServletResponse response, String forwardURL) throws IOException, ServletException {
		RequestDispatcher dispatcher = request.getRequestDispatcher(forwardURL);
		dispatcher.forward(request, response);
	}
}
