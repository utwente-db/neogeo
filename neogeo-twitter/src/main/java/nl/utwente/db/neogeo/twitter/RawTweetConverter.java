package nl.utwente.db.neogeo.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;



public class RawTweetConverter {
	
	private static final String CONFIG_FILENAME = "data_machine.properties";
	
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
			System.out.println("#!KEY1="+twitter_key1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static String schema_out = "public";
	
	private static void createOutTweetTable(Connection c, String name) throws SQLException {
		SqlUtils.executeNORES(c,
				"CREATE TABLE " + schema_out + "." + name + " (" +
						"id_str character varying(25) NOT NULL PRIMARY KEY," +
						"tweet text,"+
						"user_name text," +
						"place_name text," +
						"time timestamp with time zone," +
						"reply_to text," +
						"place_id bigint," +
						"len bigint" +
				");"
		);
		SqlUtils.execute(c,
				"SELECT AddGeometryColumn('"+schema_out+"','"+name+"','coordinates','"+Twitter.TWITTER_SRID+"','POINT',2);"
		);
	}

	private static void dropOutTweetTable(Connection c, String name) throws SQLException {
		try {
			SqlUtils.execute(c,
				"SELECT DropGeometryColumn('"+schema_out+"','"+name+"','coordinates');"); 
			} catch (SQLException e) {
				System.out.println("IGNORE: "+e);
			}
		SqlUtils.dropTable(c,schema_out,name);
	}
	
	private static PreparedStatement istat=null, istat_null = null;
	
	private static void createTweetInsertStatements(Connection c_out, String name) throws SQLException  {
		istat = c_out.prepareStatement("INSERT INTO " +
				 schema_out + "." + name + "  (id_str, tweet, place_id, time, user_name, place_name, coordinates, reply_to, len) " + 
				 "VALUES" + "  (?, ?, ?, ?, ?, ?, ST_SetSRID(ST_GeomFromText(?),"+Twitter.TWITTER_SRID+"), ?, ?);");
		istat_null = c_out.prepareStatement("INSERT INTO " +
				 schema_out + "." + name + "  (id_str, tweet, place_id, time, user_name, place_name, coordinates, reply_to, len) " + 
				 "VALUES" + "  (?, ?, ?, ?, ?, ?, ?, ?, ?);");
	}
	private static void convert(Connection c_in, Connection c_out) throws SQLException {
		String out_name = "nl_myconvert";
		
		if ( true ) {
			if ( SqlUtils.existsTable(c_out, schema_out, out_name) )
				dropOutTweetTable(c_out, out_name);
			createOutTweetTable(c_out, out_name);		
		}
		createTweetInsertStatements(c_out, out_name);
		ResultSet rs = SqlUtils.execute_big_read(c_in,"SELECT json FROM nl_raw LIMIT 2;");
		while ( rs.next() ) {
			String tweet_json = rs.getString(1);
			
			System.out.println(tweet_json);
			
			int ntweets = 0;
			Tweet t = null;
			
			try {
				t = new Tweet(tweet_json);
			} catch (Exception e) {
				System.out.println("#!Tweet(): Exception: "+e);
				System.out.println("#!Skipping....");
				continue;
			}
			String coord = t.coordinatesValue(); // incomplete, maybe filter nulls?
			PreparedStatement stat = (coord==null)?istat_null:istat;
			if ( coord != null ) {
				ntweets++;
				insertTweetTuple(
					stat, 
					t.id_str(), 
					t.tweet(), 
					-9999, // pt.insertPlace(t),
					t.created_at(),
					t.user_screen_name(), 
					t.place_full_name(),
					coord,
					t.in_reply_to_status_id()
				);
			}
		}
		
	}
	
	private static void insertTweetTuple(PreparedStatement istat, String id_str, String tweet, long place_id, Date created_at, String user_name, String place_full_name, String coordinates, String reply_to) throws SQLException {
		istat.setString(1, id_str);
		istat.setString(2, tweet);
		istat.setLong(3,place_id);
		Timestamp timestamp= new Timestamp(created_at.getTime());
		istat.setTimestamp(4, timestamp);
		istat.setString(5, user_name);
		istat.setString(6, place_full_name);
		if ( coordinates != null )
			istat.setString(7, coordinates); // String argument to geometry constructor
		else
			istat.setObject(7,null); // because now it should be a geometry object
		istat.setString(8, reply_to);
		istat.setLong(9, tweet.length()); //temporary for sum testing
		//
		istat.executeUpdate();
	}
	
	public static void main(String[] argv) throws Exception {
		System.out.println("Converter start");
		
		readProperties();
		DbConnection.connector.resetProperties(hostname,port,username,password,database);
		Connection c_in = DbConnection.connector.getConnection();
		convert(c_in, c_in);
		
	}
	
}
