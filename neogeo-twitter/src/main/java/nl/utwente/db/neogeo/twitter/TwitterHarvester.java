package nl.utwente.db.neogeo.twitter;

/* 
 * Author: Zhemin Zhu
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;

import oauth.signpost.OAuthConsumer;
import oauth.signpost.basic.DefaultOAuthConsumer;

public class TwitterHarvester {

	private static final String CONFIG_FILENAME = "data_machine.properties";
	static final String uk_coordinates = "-11.887207,50.021858,2.197266,59.040555";
	static final String nl_coordinates = "3.361816,51.316881,7.229004,53.291489";

	private static String hostname;
	private static String port;
	private static String username;
	private static String password;
	private static String database;
	private static String twitter_key1;
	private static String twitter_key2;
										
	private static void readProperties() {
		Properties prop = new Properties();
		try {
			InputStream is =
				(new TwitterHarvester()).getClass().getClassLoader().getResourceAsStream(CONFIG_FILENAME);
			prop.load(is);
			
			hostname = prop.getProperty("hostname");
			port = prop.getProperty("port");
			username = prop.getProperty("username");
			password = prop.getProperty("password");
			database = prop.getProperty("database");
			twitter_key1 = prop.getProperty("twitter_key1");
			twitter_key2 = prop.getProperty("twitter_key2");
			System.out.println("#!KEY1="+twitter_key1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
public static void tweetFilter(String country) {
		// System.out.println("KEY1="+twitter_key1);
		OAuthConsumer consumer = new DefaultOAuthConsumer(
				 "8NN6sOuTfPzKXtwLymc8Q", 
				 twitter_key1);
				
	   
		consumer.setTokenWithSecret(
				"388854051-0xSqD1eCxOLXcESefOhisgpJdXf5lw0qjjFvkNY0",
	    		  twitter_key2);
		
		HttpsURLConnection conn = null;
		BufferedReader rd = null;
		try {
			String harvest_options = "";
			
			if ( country.equals("nl")) {
				// harvest_options = "?locations=3.361816,51.316881,7.229004,53.291489";
				harvest_options = "?locations=3.361816,50.9,7.229004,53.5";
			}
			// prepare the sql statement
			/*if (country.equals("uk"))
				stmt_insert = dbConn
						.prepareStatement("insert into uk_tweet_filter_raw VALUES (?, ?, ?, ?, ?)");
			else if (country.equals("nl"))
				stmt_insert = dbConn
						.prepareStatement("insert into tweet_filter_raw VALUES (?, ?, ?, ?, ?)");*/

			// build up the web connection
			URL url = new URL("https://stream.twitter.com/1.1/statuses/filter.json" + harvest_options);
			 // "locations=3.361816,51.316881,7.229004,53.291489");
			//"?track=twitter");
			
			// construct a connection object, but not really establish the
			// connection to the server
			conn = (HttpsURLConnection) url.openConnection();
			consumer.sign(conn);
			// set the HTTP method
			conn.setRequestMethod("GET");
			// send data
			conn.setReadTimeout(1000000);
			//conn.setDoOutput(true);
			/*OutputStreamWriter wr = new OutputStreamWriter(
					conn.getOutputStream());
			String data = null;
			if (country.equals("uk"))
				data = URLEncoder.encode("locations", "UTF-8") + "="
						+ URLEncoder.encode(uk_coordinates, "UTF-8");
			else if (country.equals("nl"))
				data = URLEncoder.encode("locations", "UTF-8") + "="
						+ URLEncoder.encode(nl_coordinates, "UTF-8");

			wr.write(data);
			wr.flush();*/
			// read the result returned from the server
			rd = new BufferedReader(
					new InputStreamReader(conn.getInputStream()));
			TwitterDatabase db = new TwitterDatabase(DbConnection.connector.getConnection());
			String line = null;
			while ((line = rd.readLine()) != null) {
				db.addTweet(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			conn.disconnect();
		}
	}

	public static void main(String[] args) {
		readProperties();
		DbConnection.connector.resetProperties(hostname,port,username,password,database);
		tweetFilter("nl");
	}

}