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
import nl.utwente.db.neogeo.twitter.Tweet;

@MultipartConfig
public class AddTweetServlet extends HttpServlet {

	private static final boolean respond2enai = true;
	
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
        	// TODO: check if tweeyt is valid
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
        	} catch (SQLException e) {
        		System.out.println("#!CAUGHT: "+e);
        		response.sendError(500, "Error during enrichment phase");
            	return;
        	}	
        	if ( respond2enai ) {
        		try {
        			System.out.println("#!ENRICHED2ENAI: "+enriched);
        			HttpUtils.postTweet("http://84.35.254.52:30000/Enrichment", enriched, "UTF-8");
        			register_enai_enrichment(c,tweet.id_str(), enriched, 0);
        		} catch (Exception e) {
        			System.out.println("#!CAUGHT: "+e);
        		}
        	} else {
        		System.out.println("#! DO NOT SEND TWEET TO ENAI: "+enriched);
        	}
        } catch (Exception e) {
        	System.out.println("EXCEPTION HANDLING TWEET: "+e +",tweet="+jsonbuff);
        	response.sendError(400, "TWEET PARSE ERROR: "+e);
        	return;
        }
        // System.out.println("TWEET: id="+tweet.id_str()+", tweet="+tweet.tweet());
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
        // st.executeUpdate("DROP TABLE "+enai_table+";"); // INCOMPLETE, remove in final version
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
	
	public static String exampleTweet = "{\"truncated\":false,\"text\":\"Need \\u25ba to sort out the home broadband, its becoming far too temperamental for my liking! #unamused\",\"in_reply_to_user_id_str\":null,\"geo\":null,\"entities\":{\"hashtags\":[{\"text\":\"unamused\",\"indices\":[87,96]}],\"user_mentions\":[],\"urls\":[]},\"contributors\":null,\"place\":{\"url\":\"http://api.twitter.com/1/geo/id/53afecc4e1db9a21.json\",\"country_code\":\"GB\",\"country\":\"United Kingdom\",\"attributes\":{},\"full_name\":\"Havering, London\",\"name\":\"Havering\",\"id\":\"53afecc4e1db9a21\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[0.137939,51.484156],[0.334433,51.484156],[0.334433,51.635922],[0.137939,51.635922]]]},\"place_type\":\"city\"},\"coordinates\":null,\"source\":\"web\",\"favorited\":false,\"id_str\":\"124780554138193920\",\"retweet_count\":0,\"in_reply_to_screen_name\":null,\"in_reply_to_user_id\":null,\"created_at\":\"Tue Oct 11 15:22:38 +0000 2011\",\"user\":{\"listed_count\":2,\"geo_enabled\":true,\"friends_count\":249,\"profile_sidebar_border_color\":\"EEEEEE\",\"url\":null,\"profile_image_url\":\"http://a2.twimg.com/profile_images/1575195668/image_normal.jpg\",\"lang\":\"en\",\"profile_use_background_image\":true,\"favourites_count\":3,\"profile_text_color\":\"333333\",\"description\":\"red hair, blue eyes - witty banter and daily sarcasm from 9 till 5.\",\"location\":\"Nashville \",\"default_profile_image\":false,\"statuses_count\":4478,\"profile_background_image_url\":\"http://a1.twimg.com/profile_background_images/325298603/stripes.jpg\",\"default_profile\":false,\"following\":null,\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/325298603/stripes.jpg\",\"profile_link_color\":\"038543\",\"followers_count\":279,\"verified\":false,\"notifications\":null,\"screen_name\":\"SBRAWN\",\"id_str\":\"220478140\",\"show_all_inline_media\":true,\"follow_request_sent\":null,\"contributors_enabled\":false,\"profile_background_color\":\"ACDED6\",\"protected\":false,\"profile_background_tile\":true,\"created_at\":\"Sat Nov 27 22:34:38 +0000 2010\",\"name\":\"shannonjbrawn\",\"time_zone\":null,\"profile_sidebar_fill_color\":\"F6F6F6\",\"id\":22478140,\"is_translator\":false,\"utc_offset\":null,\"profile_image_url_https\":\"https://si0.twimg.com/profile_images/1575195668/image_normal.jpg\"},\"retweeted\":false,\"in_reply_to_status_id\":null,\"id\":123780554138193920,\"in_reply_to_status_id_str\":null}";

	public static String exampleTweet2 = "{\"truncated\":false,\"text\":\"Need \\u25ba to sort out the home broadband, its becoming far too temperamental for my liking! #unamused\",\"in_reply_to_user_id_str\":null,\"geo\":null,\"entities\":{\"hashtags\":[{\"text\":\"unamused\",\"indices\":[87,96]}],\"user_mentions\":[],\"urls\":[]},\"contributors\":null,\"place\":{\"url\":\"http://api.twitter.com/1/geo/id/53afecc4e1db9a21.json\",\"country_code\":\"GB\",\"country\":\"United Kingdom\",\"attributes\":{},\"full_name\":\"Havering, London\",\"name\":\"Havering\",\"id\":\"53afecc4e1db9a21\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[0.137939,51.484156],[0.334433,51.484156],[0.334433,51.635922],[0.137939,51.635922]]]},\"place_type\":\"city\"},\"coordinates\":null,\"source\":\"web\",\"favorited\":false,\"id_str\":\"124780554138193920\",\"retweet_count\":0,\"in_reply_to_screen_name\":null,\"in_reply_to_user_id\":null,\"created_at\":\"Tue Oct 11 15:22:38 +0000 2011\",\"user\":{\"listed_count\":2,\"geo_enabled\":true,\"friends_count\":249,\"profile_sidebar_border_color\":\"EEEEEE\",\"url\":null,\"profile_image_url\":\"http://a2.twimg.com/profile_images/1575195668/image_normal.jpg\",\"lang\":\"en\",\"profile_use_background_image\":true,\"favourites_count\":3,\"profile_text_color\":\"333333\",\"description\":\"red hair, blue eyes - witty banter and daily sarcasm from 9 till 5.\",\"location\":\"Nashville \",\"default_profile_image\":false,\"statuses_count\":4478,\"profile_background_image_url\":\"http://a1.twimg.com/profile_background_images/325298603/stripes.jpg\",\"default_profile\":false,\"following\":null,\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/325298603/stripes.jpg\",\"profile_link_color\":\"038543\",\"followers_count\":279,\"verified\":false,\"notifications\":null,\"screen_name\":\"SBRAWN\",\"id_str\":\"220478140\",\"show_all_inline_media\":true,\"follow_request_sent\":null,\"contributors_enabled\":false,\"profile_background_color\":\"ACDED6\",\"protected\":false,\"profile_background_tile\":true,\"created_at\":\"Sat Nov 27 22:34:38 +0000 2010\",\"name\":\"shannonjbrawn\",\"time_zone\":null,\"profile_sidebar_fill_color\":\"F6F6F6\",\"id\":22478140,\"is_translator\":false,\"utc_offset\":null,\"profile_image_url_https\":\"https://si0.twimg.com/profile_images/1575195668/image_normal.jpg\"},\"retweeted\":false,\"in_reply_to_status_id\":null,\"id\":123780555138193920,\"in_reply_to_status_id_str\":null}";

}

