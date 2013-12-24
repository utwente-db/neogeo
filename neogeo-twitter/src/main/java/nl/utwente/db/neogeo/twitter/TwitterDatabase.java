package nl.utwente.db.neogeo.twitter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class TwitterDatabase {
	
	private final boolean verbose = false;
	
	private Connection c = null;
	private final String schema = "public";
	private final String live_table = "nl_live";
	private final String all_table = "nl_all";
	private final String raw_table = "nl_raw";
	
	private int count = 0, no_coord = 0, skipped = 0;
	private String liveSize = "1 hour";
	// private String liveSize = "2 hours";
	private long lastLiveDelete = 0;
	private static long deltaLiveDelete = 5 * 60 * 1000; // 5 minutes
	
	TwitterDatabase(Connection c) throws SQLException {
		this.c = c;
		checkLogTable();
		checkRawTable(schema,raw_table);
		checkTweetTable(schema,all_table);
		checkTweetTable(schema,live_table);
		createInsertStatements();
		//
		// lastLiveDelete = System.currentTimeMillis();
		// 
		logMessage("Harvester started.");
	}
	
	private void checkLiveDelete() throws SQLException {
		long delta = System.currentTimeMillis() - lastLiveDelete;
		if ( delta > deltaLiveDelete ) {
			String sql = "DELETE FROM " + schema + "." + live_table + " WHERE time < (CURRENT_TIMESTAMP - interval \'"+liveSize+"\');";
			SqlUtils.executeNORES(c, sql);
			lastLiveDelete = System.currentTimeMillis();
		}
	}
	
	public void addTweet(String json) throws SQLException {
		try {
			Tweet t = null;
			
			try {
				if ( json.length() == 0 ) {
					logMessage("SKIPPING EMPTY[]:");
					return;
				}
				t = new Tweet(json);
			} catch (Exception e) {
				logMessage("SKIPPING["+e+"]:"+json);
				skipped++;
				return;
			}
			if ( verbose )
				System.out.println( t.id() + " = " + t.text());
			insertRawTuple(t.id(),t.getJson());
			String coord = t.coordinatesValue();
			if ( coord == null ) {
				coord = t.centre_place_bbox();
//				if ( coord != null )
//					System.out.println("#! CENTRE-COORD: "+coord);
			}
			// PreparedStatement stat = (coord==null)?live_stat_null:live_stat;
			if ( coord != null ) { // incomplete, check?
				insertTweetTuple(
					live_stat,
					live_stat_null,
					t.id_str(), 
					t.tweet(), 
					t.created_at(),
					t.user_screen_name(), 
					t.place_full_name(),
					coord,
					t.in_reply_to_status_id()
				);
				insertTweetTuple(
					all_stat,
					all_stat_null,
					t.id_str(), 
					t.tweet(), 
					t.created_at(),
					t.user_screen_name(), 
					t.place_full_name(),
					coord,
					t.in_reply_to_status_id()
				);
			} else {
				no_coord++;
				System.out.println("#! NO-COORD: "+t.getJson());
			}
			checkLiveDelete();
			//
			if ( (++count % 10000) == 0 )
				logMessage("Status: count="+count+", no_coord="+no_coord+", skipped="+skipped+".");
		} catch (Exception e) {
			logMessage("SKIPPING["+e+"]:"+json);
			skipped++;
		}
	}
	
	private void checkLogTable() throws SQLException {
		if ( SqlUtils.existsTable(c, schema, "harvest_log") )
			SqlUtils.dropTable(c,schema,"harvest_log");
		SqlUtils.executeNORES(c,
			"CREATE TABLE " + schema + "." + "harvest_log" + " (" +
						  "time timestamp with time zone," +
						  "message text" +
			 ");");
	}
	
	
	private void checkRawTable(String schema, String table) throws SQLException {
		if ( !SqlUtils.existsTable(c, schema, table) ) {
			SqlUtils.executeNORES(c,
				"CREATE TABLE " + schema + "." + table + " (" +
						"id bigint NOT NULL PRIMARY KEY," +
						"json text" +
				");");
		}
	}
	
	private void checkTweetTable(String schema, String table) throws SQLException {
		if ( !SqlUtils.existsTable(c, schema, table) ) {
			SqlUtils.executeNORES(c,
				"CREATE TABLE " + schema + "." + table + " (" +
						"id_str character varying(25) NOT NULL PRIMARY KEY," +
						"tweet text,"+
						"user_name text," +
						"place_name text," +
						"time timestamp with time zone," +
						"reply_to text," +
						"len bigint" +
				");");
			SqlUtils.execute(c,
				"SELECT AddGeometryColumn('"+schema+"','"+table+"','coordinates','"+Twitter.TWITTER_SRID+"','POINT',2);");
			SqlUtils.executeNORES(c,
					"CREATE INDEX ON "+schema+"."+table+" using gist(coordinates);");
		}
	}
	
	private void dropTweetTable(String schema, String table) throws SQLException {
		try {
			SqlUtils.execute(c,
				"SELECT DropGeometryColumn('"+schema+"','"+table+"','coordinates');"); 
			} catch (SQLException e) {
				System.out.println("IGNORE: "+e);
			}
		SqlUtils.dropTable(c,schema,table);
	}
	
	PreparedStatement live_stat = null, live_stat_null = null, all_stat = null, all_stat_null = null, rstat = null, lstat = null;
	
	private void createInsertStatements() throws SQLException  {
		lstat = c.prepareStatement("INSERT INTO " +
				 schema + "." + "harvest_log" + "  (time, message) " + "VALUES" + "  (CURRENT_TIMESTAMP, ?);");
		rstat = c.prepareStatement("INSERT INTO " +
				 schema + "." + raw_table + "  (id, json) " + "VALUES" + "  (?, ?);");
		live_stat = c.prepareStatement("INSERT INTO " +
				 schema + "." + live_table + "  (id_str, tweet, time, user_name, place_name, coordinates, reply_to, len) " + 
				 "VALUES" + "  (?, ?, ?, ?, ?, ST_SetSRID(ST_GeomFromText(?),"+Twitter.TWITTER_SRID+"), ?, ?);");
		live_stat_null = c.prepareStatement("INSERT INTO " +
				 schema + "." + live_table + "  (id_str, tweet, time, user_name, place_name, coordinates, reply_to, len) " + 
				 "VALUES" + "  (?, ?, ?, ?, ?, ?, ?, ?);");
		all_stat = c.prepareStatement("INSERT INTO " +
				 schema + "." + all_table + "  (id_str, tweet, time, user_name, place_name, coordinates, reply_to, len) " + 
				 "VALUES" + "  (?, ?, ?, ?, ?, ST_SetSRID(ST_GeomFromText(?),"+Twitter.TWITTER_SRID+"), ?, ?);");
		all_stat_null = c.prepareStatement("INSERT INTO " +
				 schema + "." + all_table + "  (id_str, tweet, time, user_name, place_name, coordinates, reply_to, len) " + 
				 "VALUES" + "  (?, ?, ?, ?, ?, ?, ?, ?);");
	}
	
	private void logMessage(String message) throws SQLException {
		lstat.setString(1,message);
		lstat.executeUpdate();
	}

	private void insertRawTuple(Long id, String json) throws SQLException {
		rstat.setLong(1, id.longValue());
		rstat.setString(2, json);
		rstat.executeUpdate();
	}

	private void insertTweetTuple(PreparedStatement coord_stat, PreparedStatement null_stat, String id_str, String tweet, Date created_at, String user_name, String place_full_name, String coordinates, String reply_to) throws SQLException {
		PreparedStatement stat = (coordinates==null)?null_stat:coord_stat;
		stat.setString(1, id_str);
		stat.setString(2, tweet);
		Timestamp timestamp= new Timestamp(created_at.getTime());
		stat.setTimestamp(3, timestamp);
		stat.setString(4, user_name);
		stat.setString(5, place_full_name);
		if ( coordinates != null )
			stat.setString(6, coordinates); // String argument to geometry constructor
		else
			stat.setObject(6,null); // because now it should be a geometry object
		stat.setString(7, reply_to);
		stat.setLong(8, tweet.length()); //temporary for sum testing
		//
		stat.executeUpdate();
	}
}