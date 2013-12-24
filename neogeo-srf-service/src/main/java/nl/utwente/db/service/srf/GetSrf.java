package nl.utwente.db.service.srf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@MultipartConfig
public class GetSrf extends HttpServlet {
	
    private static final long serialVersionUID = 1L;
   
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/html");
        
        System.out.println("GetSrf:doGet(): called");
        StringBuffer jsonbuff = new StringBuffer();
        String line = null;
        try {
          BufferedReader reader = request.getReader();
          while ((line = reader.readLine()) != null)
            jsonbuff.append(line);
        } catch (Exception e) { 
        	e.printStackTrace(); /*report an error*/ 
        }
        
        String par = jsonbuff.toString();
        String rsp = "<html><head><title>OKIDOKI-2</title><body>"+par+"</body></head></html>";

        PrintWriter writer = response.getWriter();
        writer.write(rsp);
        
        try {
        	// TODO: check if tweet is valid
        	
//        	Connection c = getGeonamesConnection();
//        	if ( c == null ) {
//        		response.sendError(500, "Unable to connect to geonames database");
//            	return;
//        	} 
//        	String enriched = null;
//        	try {
//        		register_enai_tweet(c,tweet.id_str(), tweet.getJson());
//        		enriched = TweetHandler.enrichTweet(c, tweet);
//        	} catch (SQLException e) {
//        		System.out.println("#!CAUGHT: "+e);
//        		response.sendError(500, "Error during enrichment phase");
//            	return;
//        	}	
//        	if ( respond2enai ) {
//        		try {
//        			System.out.println("#!ENRICHED2ENAI: "+enriched);
//        			HttpUtils.postTweet("http://84.35.254.52:30000/Enrichment", enriched, "UTF-8");
//        			register_enai_enrichment(c,tweet.id_str(), enriched, 0);
//        		} catch (Exception e) {
//        			System.out.println("#!CAUGHT: "+e);
//        		}
//        	} else {
//        		System.out.println("#! DO NOT SEND TWEET TO ENAI: "+enriched);
//        	}
        } catch (Exception e) {
        	System.out.println("EXCEPTION HANDLING TWEET: "+e +",tweet="+jsonbuff);
        	response.sendError(400, "TWEET PARSE ERROR: "+e);
        	return;
        }
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        doGet(request, response);
	}
	
}

