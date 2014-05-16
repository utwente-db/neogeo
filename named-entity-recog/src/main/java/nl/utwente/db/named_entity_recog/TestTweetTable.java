package nl.utwente.db.named_entity_recog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import nl.utwente.db.neogeo.twitter.Tweet;

public class TestTweetTable
{
	public static final String tttSchema = "public";
	public static final String tttTable = "tweets";
	
	Connection c;
	
	public TestTweetTable(Connection c) {
		this.c = c;
	}
	
	public void buildFromResultSet(ResultSet rs) throws SQLException {
		buildTable();
		PreparedStatement ps = c.prepareStatement("INSERT INTO " + tttSchema + "." + tttTable + "  (" +
				// "postalcode," +
				"id," +
				"tweet" +
		") VALUES(?,?);");
		int count = 0;
		while ( rs.next() ) {
			String id = rs.getString(1);
			String raw = rs.getString(2);
			Tweet t = new Tweet(raw);
			if ( t.isValid() ) {
				// System.out.println(t.text());
				ps.setString(1, id);
				ps.setString(2, t.text());
				ps.execute();
			}
			if ( (++count % 1000) == 0 ) {
				System.out.print(".");
				System.out.flush();
			}
		}
	}
	
	public void buildTable() throws SQLException {
		if ( SqlUtils.existsTable(c, tttSchema, tttTable) )
			SqlUtils.dropTable(c,tttSchema, tttTable);
		SqlUtils.executeNORES(c, 
				"CREATE TABLE " + tttSchema + "." + tttTable + " (" +
				"id varchar(20) PRIMARY KEY," +
				"tweet TEXT" +
		");");
	}
	
	public void generate(String tsname, String like, int limit) throws SQLException {
		TestTweetTable builder = new TestTweetTable( c );
		c.setAutoCommit(false);
		Statement st = c.createStatement();
		st.setFetchSize(10);
		String select = "select id,json_tweet from enai_tweet ";
		String where = "";
		if ( like != null && like.length() > 0 ) {
			where = " WHERE json_tweet LIKE \'"+like+"\'";
		}
		ResultSet rs = st.executeQuery(select + where + " LIMIT "+limit+";");
		builder.buildFromResultSet(rs);
		c.setAutoCommit(true);
	}
	
	public ResultSet startTestTweets(String tsname) throws SQLException {
		// c.setAutoCommit(false);
		Statement st = c.createStatement();
		st.setFetchSize(10);
		return st.executeQuery("select id,tweet from "+tsname+";");
	}
	
	public void stopTestTweets(ResultSet rs) throws SQLException {
		// c.setAutoCommit(true);
	}
	
	
	public static void main(String[] args)
    {
		try {
			TestTweetTable ttt = new TestTweetTable( GeoNamesDB.geoNameDBConnection() );
			ttt.generate(tttTable,"%straat%",25);
			
			if ( true ) {
				ResultSet rs = ttt.startTestTweets(tttTable);
				
				while( rs.next() ) {
					System.out.println(rs.getString(2));
				}
				ttt.stopTestTweets(rs);
			}
		} catch (Exception e) {
			System.out.println("#CAUGHT: "+e);
			e.printStackTrace();
		}
    }
}