package nl.utwente.db.twitter.server;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.utwente.db.named_entity_recog.RequestHandler;

@MultipartConfig
public class GeoEntityFinder extends HttpServlet {

    private static final long serialVersionUID = 1L;
    
    RequestHandler handler = new RequestHandler();
    
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/json");
        
        PrintWriter writer = response.getWriter();

        int status = 0;
        try {
        	status = handler.handleGeoEntityFinder(request.getReader(), writer);
        } catch (Exception e) {
        	e.printStackTrace();
        	response.sendError(500, "Exception:"+e);
        }
        
        if ( status != 0 )
        	System.out.println("BAD STATUS");
        	response.sendError(status, "incomplete internal error");
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        doGet(request, response);
    }
    
}

