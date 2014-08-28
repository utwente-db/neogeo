package nl.utwente.db.neogeo.twitter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Iterator;

public class TweetConverter {

	class PgJsonIterator implements Iterator<String> {
		
		boolean		more;
		ResultSet	rs;
		
		PgJsonIterator(Connection c, String sql_select) throws SQLException {
			rs  = SqlUtils.execute_big_read(c_in, sql_select);
			more = rs.next(); // incomplete, don't we skip the first here?
		}
		
		public boolean	hasNext() { return more; }
        
		public String	next() {
			String res = null;
			
			if ( rs != null && more ) {
				try {
					res = rs.getString(1);
					more = rs.next(); // advance the cursor
				} catch(SQLException e) {
					e.printStackTrace();
				}
			}
			return res;
		}
		
		public void	remove() {
			throw new RuntimeException("PgJsonIterator:remove: unexpected!");
        }
		
	}
	
	class FileJsonIterator implements Iterator<String> {

		private String 			nextElement = null;
		private BufferedReader	br;

		FileJsonIterator(String fileName) throws IOException {		
				String line;

				InputStream fis = new FileInputStream(fileName);
				br = new BufferedReader(new InputStreamReader(fis,Charset.forName("UTF-8")));
				boolean started = false;
				while (!started && (line = br.readLine()) != null) {
					if (line.startsWith("COPY tweet_filter_raw"))
						started = true;
				}
				if (!started) {
					System.out.println("ERR: ");
					throw new IOException("FileJsonIterator: bad json input file");
				}
				nextElement = findNextElement();
		}

		private String findNextElement() throws IOException {
			String line = br.readLine();
			
			if ( line != null ) {
				if (! line.startsWith("\\.")) {
					int json_start = line.indexOf("\t{");
					if ( json_start > 0 ) {
						line = line.substring(json_start+1);
						// line = URLDecoder.decode(line, "UTF-8");
						line = line.replace("\\\\", "\\");
						return line;
					} else
						System.err.println("FAIL: [" + line + "]");
				}
			}
			return null;
		}

		public boolean hasNext() {
			return nextElement != null;
		}
		
		public String next() {
			String res = nextElement;
			try {
				nextElement = findNextElement();
			} catch (IOException e) {
				e.printStackTrace();
				nextElement = null;
			}
			return res;
		}

		public void remove() {
			throw new RuntimeException("FileJsonIterator:remove: unexpected!");
		}

	}
	
	private String inputFile; // the optional file where the tweets are read from
	private Connection c_in;
	private Connection c_out;
	private String schema_in;
	private String schema_out;
	private String rawTable;
	private String tweetTable;
	private String placeTable;
	private PreparedStatement istat;
	private PreparedStatement istat_null;
	
	public TweetConverter(Connection c_in, String schema_in, String rawTable, Connection c_out, String schema_out, String base) 
			throws SQLException {
		this.inputFile = null;
		this.c_in = c_in;
		this.c_out = c_out;
		this.schema_in = schema_in;
		this.schema_out = schema_out;
		this.rawTable = rawTable;
		this.tweetTable = base + "_neogeo";
		this.placeTable = base + "_place";
		//
		convertTweets();
	}
	
	public TweetConverter(String inputFile, Connection c_out,
			String schema_out, String base) throws SQLException {
		this.inputFile = inputFile;
		this.c_in = null;
		this.c_out = c_out;
		this.schema_in = null;
		this.schema_out = schema_out;
		this.rawTable = null;
		this.tweetTable = base + "_neogeo";
		this.placeTable = base + "_place";
		//
		convertTweets();
	}
	
	public void convertTweets() throws SQLException {
		long start_ms = new Date().getTime();
		
		if (SqlUtils.existsTable(c_out, schema_out, tweetTable)) 
			dropTweetTable();
		createTweetTable();
		createTweetInsertStatements();
		PlaceTable pt = new PlaceTable(c_out,schema_out,placeTable,true);
		long count = 0;
		long ntweets =0, nskipped = 0;
		
		Iterator<String> tw_it = null;
		if ( inputFile != null ) {
			try {
				System.out.println("#!Converting twitter file: " + inputFile);
				tw_it = new FileJsonIterator(inputFile);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
		} else {
			System.out.println("#!Converting PG twitter table: " + rawTable);
			tw_it = new PgJsonIterator(c_in, "SELECT json FROM " + schema_in
					+ "." + rawTable + ";");
		}
		while (tw_it.hasNext()) {
			String json_tweet = tw_it.next();
			
			if ( json_tweet == null ) {
				System.out.println("TweetConverter:convertTweets: error getting tweets, break()");
				break;
			}
			// System.out.println("JSON: "+json_tweet);
			Tweet t;
			try {
				t = new Tweet(json_tweet);
			} catch (Exception e) {
				System.out.println("#!Tweet(): Exception: "+e);
				System.out.println("#!Skipping....");
				continue;
			}
			if (++count % 10000 == 0)
				System.out.println("Handled " + count + " tweets");
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
			} else
				nskipped++;
		}
		istat.close();
		pt.endInsert();
		SqlUtils.create_index(c_out,schema_out,tweetTable,"GIST","coordinates");
		long elapsed_ms = new Date().getTime() - start_ms;
		System.out.println("#!Finished (#total="+(ntweets+nskipped)+", #tweets="+ntweets+", #skipped="+nskipped+", time="+(long)(elapsed_ms/1000)+"s)");
	}
	
	private void createTweetTable() throws SQLException {
		SqlUtils.executeNORES(c_out,
				"CREATE TABLE " + schema_out + "." + tweetTable + " (" +
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
		SqlUtils.execute(c_out,
				"SELECT AddGeometryColumn('"+schema_out+"','"+tweetTable+"','coordinates','"+Twitter.TWITTER_SRID+"','POINT',2);"
		);
	}

	private void dropTweetTable() throws SQLException {
		try {
			SqlUtils.execute(c_out,
				"SELECT DropGeometryColumn('"+schema_out+"','"+tweetTable+"','coordinates');"); 
			} catch (SQLException e) {
				System.out.println("IGNORE: "+e);
			}
		SqlUtils.dropTable(c_out,schema_out,tweetTable);
	}
	
	private void createTweetInsertStatements() throws SQLException  {
		istat = c_out.prepareStatement("INSERT INTO " +
				 schema_out + "." + tweetTable + "  (id_str, tweet, place_id, time, user_name, place_name, coordinates, reply_to, len) " + 
				 "VALUES" + "  (?, ?, ?, ?, ?, ?, ST_SetSRID(ST_GeomFromText(?),"+Twitter.TWITTER_SRID+"), ?, ?);");
		istat_null = c_out.prepareStatement("INSERT INTO " +
				 schema_out + "." + tweetTable + "  (id_str, tweet, place_id, time, user_name, place_name, coordinates, reply_to, len) " + 
				 "VALUES" + "  (?, ?, ?, ?, ?, ?, ?, ?, ?);");
	}
	
	private void insertTweetTuple(PreparedStatement istat, String id_str, String tweet, long place_id, Date created_at, String user_name, String place_full_name, String coordinates, String reply_to) throws SQLException {
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
	
	public static void main(String[] argv) {
		try {
			// Connection c = HibernateUtils.getJDBCConnection();
			Connection c = DbConnection.connector.getConnection();
			// if ( false ) { exp2(c); System.exit(0); }
			// new
			// TweetConverter(c,"public","london_hav_raw",c,"public","london_hav");
			// new
			// TweetConverter("/Users/flokstra/twitter_sm.db",c,"public","london_hav");
			// new TweetConverter("/Users/flokstra/uk_raw.sql",c,"public","uk");
			// new TweetConverter("/Users/flokstra/tmp/twitter.db", c, "public", "nluk");
			c.close();
		} catch (SQLException e) {
			System.out.println("Caught: " + e);
			e.printStackTrace(System.out);
		}
	}
	
	public static void exp2(Connection c) throws SQLException {
		PlaceTable.repairGlobalPlace(c, "public", PlaceTable.GLOBAL_PLACES);
	}

}
