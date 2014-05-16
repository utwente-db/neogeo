package nl.utwente.db.twitter.server;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import nl.utwente.db.neogeo.twitter.Tweet;

public class HttpUtils {
 
	private static final String USER_AGENT = "Mozilla/5.0";
	
	private static String json_req = "{ \"area\": { \"srid\": \"4326\", \"label\": \"enschede_c\", \"cache\": \"create\", \"kind\" : \"bbox\", \"bbox\": { \"sw_lon\": \"6.9\", \"sw_lat\": \"52.2167\", \"ne_lon\": \"6.95\", \"ne_lat\": \"52.22\" } }, \"entity_kind\": [ \"Street\" ], \"result_kind\": \"list\", \"format\": \"json\", \"limit\": 100 }";			

	public static void main(String[] args) throws Exception {
		String host = "farm15.ewi.utwente.nl";
		
		if ( true ) {
			// postTweet("http://"+host+":8080/neogeo-twitter-server/AddTweet",
					// exampleTweet2, "UTF-8");
			postTweet("http://"+host+":8080/neogeo-twitter-server/GeoEntityFinder",
					json_req, "UTF-8");
		} else {
			Tweet tweet = new Tweet(Tweet.exampleTweet);			
		}
	}
 
	public static void postTweet(String url, String json_tweet, String charset) throws Exception {
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		//add request header
		con.setRequestMethod("POST");
		// con.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
		con.setRequestProperty("Content-Type", "application/json");
		con.setRequestProperty("User-Agent", USER_AGENT);
		// con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
 
		String urlParameters = json_tweet;
		if ( false && charset != null )
		     urlParameters = URLEncoder.encode(urlParameters, charset);
		
		// Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();
 
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'POST' request to URL : " + url);
		System.out.println("Post parameters : " + urlParameters);
		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
 
		//print result
		System.out.println(response.toString());
	}
	
	// HTTP GET request
	private static void sendGet() throws Exception {
 
		String url = "http://www.google.com/search?q=mkyong";
 
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		// optional default is GET
		con.setRequestMethod("GET");
 
		//add request header
		con.setRequestProperty("User-Agent", USER_AGENT);
 
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
 
		//print result
		System.out.println(response.toString());
 
	}
	
	// HTTP POST request
	private static void old_sendPost() throws Exception {
 
		String url = "https://selfsolve.apple.com/wcResults.do";
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		//add reuqest header
		con.setRequestMethod("POST");
		con.setRequestProperty("User-Agent", USER_AGENT);
		con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");
 
		String urlParameters = "sn=C02G8416DRJM&cn=&locale=&caller=&num=12345";
 
		// Send post request
		con.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		wr.writeBytes(urlParameters);
		wr.flush();
		wr.close();
 
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'POST' request to URL : " + url);
		System.out.println("Post parameters : " + urlParameters);
		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
 
		//print result
		System.out.println(response.toString());
	}
	
	public static String exampleTweet = "{\"truncated\":false,\"text\":\"Need \\u25ba to sort out the home broadband, its becoming far too temperamental for my liking! #unamused\",\"in_reply_to_user_id_str\":null,\"geo\":null,\"entities\":{\"hashtags\":[{\"text\":\"unamused\",\"indices\":[87,96]}],\"user_mentions\":[],\"urls\":[]},\"contributors\":null,\"place\":{\"url\":\"http://api.twitter.com/1/geo/id/53afecc4e1db9a21.json\",\"country_code\":\"GB\",\"country\":\"United Kingdom\",\"attributes\":{},\"full_name\":\"Havering, London\",\"name\":\"Havering\",\"id\":\"53afecc4e1db9a21\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[0.137939,51.484156],[0.334433,51.484156],[0.334433,51.635922],[0.137939,51.635922]]]},\"place_type\":\"city\"},\"coordinates\":null,\"source\":\"web\",\"favorited\":false,\"id_str\":\"124780554138193920\",\"retweet_count\":0,\"in_reply_to_screen_name\":null,\"in_reply_to_user_id\":null,\"created_at\":\"Tue Oct 11 15:22:38 +0000 2011\",\"user\":{\"listed_count\":2,\"geo_enabled\":true,\"friends_count\":249,\"profile_sidebar_border_color\":\"EEEEEE\",\"url\":null,\"profile_image_url\":\"http://a2.twimg.com/profile_images/1575195668/image_normal.jpg\",\"lang\":\"en\",\"profile_use_background_image\":true,\"favourites_count\":3,\"profile_text_color\":\"333333\",\"description\":\"red hair, blue eyes - witty banter and daily sarcasm from 9 till 5.\",\"location\":\"Nashville \",\"default_profile_image\":false,\"statuses_count\":4478,\"profile_background_image_url\":\"http://a1.twimg.com/profile_background_images/325298603/stripes.jpg\",\"default_profile\":false,\"following\":null,\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/325298603/stripes.jpg\",\"profile_link_color\":\"038543\",\"followers_count\":279,\"verified\":false,\"notifications\":null,\"screen_name\":\"SBRAWN\",\"id_str\":\"220478140\",\"show_all_inline_media\":true,\"follow_request_sent\":null,\"contributors_enabled\":false,\"profile_background_color\":\"ACDED6\",\"protected\":false,\"profile_background_tile\":true,\"created_at\":\"Sat Nov 27 22:34:38 +0000 2010\",\"name\":\"shannonjbrawn\",\"time_zone\":null,\"profile_sidebar_fill_color\":\"F6F6F6\",\"id\":22478140,\"is_translator\":false,\"utc_offset\":null,\"profile_image_url_https\":\"https://si0.twimg.com/profile_images/1575195668/image_normal.jpg\"},\"retweeted\":false,\"in_reply_to_status_id\":null,\"id\":123780554138193920,\"in_reply_to_status_id_str\":null}";

	public static String exampleTweet2 = "{\"truncated\":false,\"text\":\"Need \\u25ba to sort out the home broadband, its becoming far too temperamental for my liking! #unamused\",\"in_reply_to_user_id_str\":null,\"geo\":null,\"entities\":{\"hashtags\":[{\"text\":\"unamused\",\"indices\":[87,96]}],\"user_mentions\":[],\"urls\":[]},\"contributors\":null,\"place\":{\"url\":\"http://api.twitter.com/1/geo/id/53afecc4e1db9a21.json\",\"country_code\":\"GB\",\"country\":\"United Kingdom\",\"attributes\":{},\"full_name\":\"Havering, London\",\"name\":\"Havering\",\"id\":\"53afecc4e1db9a21\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[0.137939,51.484156],[0.334433,51.484156],[0.334433,51.635922],[0.137939,51.635922]]]},\"place_type\":\"city\"},\"coordinates\":null,\"source\":\"web\",\"favorited\":false,\"id_str\":\"124780554138193920\",\"retweet_count\":0,\"in_reply_to_screen_name\":null,\"in_reply_to_user_id\":null,\"created_at\":\"Tue Oct 11 15:22:38 +0000 2011\",\"user\":{\"listed_count\":2,\"geo_enabled\":true,\"friends_count\":249,\"profile_sidebar_border_color\":\"EEEEEE\",\"url\":null,\"profile_image_url\":\"http://a2.twimg.com/profile_images/1575195668/image_normal.jpg\",\"lang\":\"en\",\"profile_use_background_image\":true,\"favourites_count\":3,\"profile_text_color\":\"333333\",\"description\":\"red hair, blue eyes - witty banter and daily sarcasm from 9 till 5.\",\"location\":\"Nashville \",\"default_profile_image\":false,\"statuses_count\":4478,\"profile_background_image_url\":\"http://a1.twimg.com/profile_background_images/325298603/stripes.jpg\",\"default_profile\":false,\"following\":null,\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/325298603/stripes.jpg\",\"profile_link_color\":\"038543\",\"followers_count\":279,\"verified\":false,\"notifications\":null,\"screen_name\":\"SBRAWN\",\"id_str\":\"220478140\",\"show_all_inline_media\":true,\"follow_request_sent\":null,\"contributors_enabled\":false,\"profile_background_color\":\"ACDED6\",\"protected\":false,\"profile_background_tile\":true,\"created_at\":\"Sat Nov 27 22:34:38 +0000 2010\",\"name\":\"shannonjbrawn\",\"time_zone\":null,\"profile_sidebar_fill_color\":\"F6F6F6\",\"id\":22478140,\"is_translator\":false,\"utc_offset\":null,\"profile_image_url_https\":\"https://si0.twimg.com/profile_images/1575195668/image_normal.jpg\"},\"retweeted\":false,\"in_reply_to_status_id\":null,\"id\":123780555138193920,\"in_reply_to_status_id_str\":null}";

}
