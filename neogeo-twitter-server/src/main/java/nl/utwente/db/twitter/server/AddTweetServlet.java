package nl.utwente.db.twitter.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.servlet.annotation.MultipartConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.utwente.db.named_entity_recog.EntityResolver;

import org.json.simple.parser.ParseException;

@MultipartConfig
public class AddTweetServlet extends HttpServlet {

	private static final boolean respond2enai = false;
	
    private static final long serialVersionUID = 1L;

    private static Connection cached_connection = null;
    
    private static Connection getGeonamesConnection() {	
    	if (cached_connection == null ) {
    		try {
    			cached_connection = EntityResolver.geonames_conn;
    			check_enai_tweet_table(cached_connection);
    		} catch (SQLException e) {
    			// WOW, troubles, cannot get to geonames db so nothing will work
    			System.out.println("ERROR: NO DB: "+e);
    			return null;
    		}

    	}
    	return cached_connection;
    }
    
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("text/html");
        String rsp = "<html><head><title>OK</title></head></html>";

        PrintWriter writer = response.getWriter();
        writer.write(rsp);
        
        StringBuffer jsonbuff = new StringBuffer();
        String line = null;
        try {
          BufferedReader reader = request.getReader();
          while ((line = reader.readLine()) != null)
            jsonbuff.append(line);
        } catch (Exception e) { e.printStackTrace(); /*report an error*/ }

        Tweet tweet = null;
        
        try {
        	tweet = new Tweet(jsonbuff.toString());
        	
        	Connection c = getGeonamesConnection();
        	if ( c == null ) {
        		response.sendError(500, "Unable to connect to geonames database");
            	return;
        	}
        	String enriched = null;
        	try {
        		register_enai_tweet(c,tweet.id_str(), tweet.getJson());
        		enriched = TweetHandler.enrichTweet(c, tweet);
        		// register_enai_enrichment(c,tweet.id_str(), enriched, 0);
        	} catch (SQLException e) {
        		System.out.println("#!CAUGHT: "+e);
        		response.sendError(500, "Error during enrichment phase");
            	return;
        	}
        	
        	System.out.println("#!ENRICHED: "+enriched);
        	if ( respond2enai ) {
        		try {
        			HttpUtils.postTweet("http://84.35.254.52:30000/Enrichment", enriched, "UTF-8");
        			register_enai_enrichment(c,tweet.id_str(), enriched, 0);
        		} catch (Exception e) {
        			System.out.println("#!CAUGHT: "+e);
        		}
        	} else {
        		System.out.println("#! DO NOT SEND TWEET TO ENAI: "+enriched);
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
    
    private static final String geonames_schema = "public";
	private static final String enia_tweet_table = "enai_tweet";
	
	private static final String enai_table = geonames_schema + "." + enia_tweet_table; 
	
	public static void check_enai_tweet_table(Connection c) throws SQLException {
        Statement st = c.createStatement();
        st.executeUpdate("DROP TABLE "+enai_table+";"); // INCOMPLETE, remove in final version
        ResultSet rs = st.executeQuery("SELECT COUNT(*) from pg_tables WHERE schemaname=\'" + geonames_schema + "\' AND tablename=\'" + enia_tweet_table + "\';");
        rs.next();
        if ( rs.getInt(1) == 0 ) {
        	// create the table
        	String sql;
        	
        	sql = "CREATE TABLE " + enai_table + " ("+
        		    "id        	char(20) PRIMARY KEY,"+
        		    "json_tweet  text,"+
        		    "json_enrich text,"+
        		    "tries		 int," +
        		    "last_error	 text," +
        		    "sent        int"+
        		");";
        	st.executeUpdate(sql);
        } else {
        	System.out.println("TABLE EXISTS");
        }

	}
	public static void register_enai_tweet(Connection c, String id, String json) throws SQLException {
		PreparedStatement ps = c.prepareStatement("INSERT INTO " + enai_table + "  (id,json_tweet,json_enrich,sent) VALUES(?,?,?,?);");
		ps.setString(1, id);
		ps.setString(2,json);
		ps.setString(3,"");
		ps.setInt(4,0);
		ps.executeUpdate();
	}
	
	public static void register_enai_enrichment(Connection c, String id, String json_enrich, int sent) throws SQLException {
		PreparedStatement ps = c.prepareStatement("UPDATE " + enai_table + "  SET json_enrich=?, sent=? WHERE id=?;");
		ps.setString(3, id);
		ps.setString(1,json_enrich);
		ps.setInt(2,sent);
		ps.executeUpdate();
	}
	
	public static void register_enai_enrichment(Connection c, String id) throws SQLException {
		PreparedStatement ps = c.prepareStatement("UPDATE " + enai_table + "  SET sent=? WHERE id=?;");
		ps.setString(2, id);
		ps.setInt(1,1);
		ps.executeUpdate();
	}
}

