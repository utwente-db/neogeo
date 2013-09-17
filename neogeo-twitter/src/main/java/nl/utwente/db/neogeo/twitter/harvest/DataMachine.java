package nl.utwente.db.neogeo.twitter.harvest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;

import nl.utwente.db.neogeo.twitter.harvest.type.filterTweet;
import sun.misc.BASE64Encoder;

public class DataMachine {

	private static final String CONFIG_FILENAME = "data_machine.properties";
	static final String uk_coordinates = "-11.887207,50.021858,2.197266,59.040555";
	static final String nl_coordinates = "3.361816,51.316881,7.229004,53.291489";
	//private String uk_username; // twitter account
	private String twitter_username; // twitter account
	private String twitter_password;       // use the same password for all
											// accounts
	private String hostname;
	private String port;
	private String username;
	private String database;
	private String password;
	private int index = 0;
	
	public DataMachine(){
		readProperties(CONFIG_FILENAME);
	}
	
	public String getCurrentTime(){
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		return dateFormat.format(date);
	}
	

	public void processFilterRecord(String record, Connection conn,
			PreparedStatement stmt_insert) {
		try {
			filterTweet st = JSONParser.parseFilterTweet(record);
			if (st.m_idstr.equals("null") || st.m_tweet.equals("null")
					|| st.m_place.equals("null") || st.m_place.equals("''")
					|| st.m_place.equals("") || st.m_time.equals("null"))
				return;
			stmt_insert.setString(1, st.m_idstr);
			stmt_insert.setString(2, st.m_tweet);
			stmt_insert.setString(3, st.m_time);
			stmt_insert.setString(4, st.m_place);
			stmt_insert.setString(5, st.m_json);
			stmt_insert.execute();
			System.out.println(index++ + "	" + getCurrentTime());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	private void readProperties(String propFilename) {
		Properties prop = new Properties();
		try {
			InputStream is =
				this.getClass().getClassLoader().getResourceAsStream(CONFIG_FILENAME);
			prop.load(is);
			hostname = prop.getProperty("hostname");
			port = prop.getProperty("port");
			username = prop.getProperty("username");
			password = prop.getProperty("password");
			database = prop.getProperty("database");
			twitter_username = prop.getProperty("twitter_user");
			twitter_password = prop.getProperty("twitter_password");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void tweetFilter(String country) {
		HttpsURLConnection conn = null;
		BufferedReader rd = null;
		PreparedStatement stmt_insert = null;
		Connection dbConn = null;
		try {
			// connect to the Postgresql database
			Class.forName("org.postgresql.Driver");
			dbConn = DriverManager.getConnection(
					"jdbc:postgresql://silo3.ewi.utwente.nl:5432/twitter", "postgres",
					"postgres");
			// prepare the sql statement
			if (country.equals("uk"))
				stmt_insert = dbConn
						.prepareStatement("insert into uk_tweet_filter_raw VALUES (?, ?, ?, ?, ?)");
			else if (country.equals("nl"))
				stmt_insert = dbConn
						.prepareStatement("insert into tweet_filter_raw VALUES (?, ?, ?, ?, ?)");

			// build up the web connection
			URL url = new URL(
					"https://stream.twitter.com/1/statuses/filter.json");
			// construct a connection object, but not really establish the
			// connection
			// to the server
			conn = (HttpsURLConnection) url.openConnection();
			// set the HTTP method
			conn.setRequestMethod("POST");
			// basic authentication
			BASE64Encoder enc = new BASE64Encoder();
			String userpassword = null;
//			if (country.equals("uk"))
//				userpassword = uk_username + ":" + password;
//			else if (country.equals("nl"))
				userpassword = twitter_username + ":" + password;

			String encodedAuth = enc.encode(userpassword.getBytes());
			conn.setRequestProperty("Authorization", "Basic " + encodedAuth);
			conn.setReadTimeout(1000000);
			// send data
			conn.setDoOutput(true);
			OutputStreamWriter wr = new OutputStreamWriter(
					conn.getOutputStream());
			String data = null;
			if (country.equals("uk"))
				data = URLEncoder.encode("locations", "UTF-8") + "="
						+ URLEncoder.encode(uk_coordinates, "UTF-8");
			else if (country.equals("nl"))
				data = URLEncoder.encode("locations", "UTF-8") + "="
						+ URLEncoder.encode(nl_coordinates, "UTF-8");

			wr.write(data);
			wr.flush();
			// read the result returned from the server
			rd = new BufferedReader(
					new InputStreamReader(conn.getInputStream()));
			StringBuilder sb = new StringBuilder();
			String line = null;
			while ((line = rd.readLine()) != null) {
				processFilterRecord(line, null, stmt_insert);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			conn.disconnect();
			try {
				dbConn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	
	
	public static void main(String[] args) {
		DataMachine m = new DataMachine();
		m.tweetFilter("nl");
	}

}
