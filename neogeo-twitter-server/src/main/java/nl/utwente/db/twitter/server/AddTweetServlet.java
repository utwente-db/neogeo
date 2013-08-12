package nl.utwente.db.twitter.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.parser.ParseException;

@MultipartConfig
public class AddTweetServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // ClassLoader classLoader = getClass().getClassLoader();
        // File file = new File(classLoader.getResource("quest.zip").getFile());

        response.setContentType("text/html");
        String rsp = "<html><head><title>OK</title></head></html>";
        // response.setContentLength(rsp.length());
        // response.setHeader("Content-Disposition", "attachment;filename=\"" + file.getName() + "\"");

        PrintWriter writer = response.getWriter();
        writer.write(rsp);
        
        StringBuffer jsonbuff = new StringBuffer();
        String line = null;
        try {
          System.out.println("INPUT char encoding is: " + request.getCharacterEncoding());
          BufferedReader reader = request.getReader();
          while ((line = reader.readLine()) != null)
            jsonbuff.append(line);
        } catch (Exception e) { e.printStackTrace(); /*report an error*/ }
        
        // System.out.println("READ JSON: "+jsonbuff);
        // String s = Tweet.exampleTweet;
        // System.out.println("OK: "+s);

        Tweet tweet = null;
        
        try {
        	tweet = new Tweet(jsonbuff.toString());
        	
        	String enriched = tweet.dummyEnriched();
        	
        	System.out.println("#!ENRICHED: "+enriched);
        	if ( true ) {
        		try {
        			HttpUtils.postTweet("http://84.35.254.52:30000/Enrichment", enriched, "UTF-8");
        		} catch (Exception e) {
        			System.out.println("#!CAUGHT: "+e);
        		}
        	}
        } catch (ParseException e) {
        	System.out.println("INVALID TWEET: "+e +",tweet="+jsonbuff);
        	response.sendError(400, "TWEET PARSE ERROR: "+e);
        	return;
        }
        System.out.println("TWEET: id="+tweet.id_str()+", tweet="+tweet.tweet());
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        doGet(request, response);
    }
}

