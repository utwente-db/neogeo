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
	
	public void generate() throws SQLException {
		TestTweetTable builder = new TestTweetTable( c );
		c.setAutoCommit(false);
		Statement st = c.createStatement();
		st.setFetchSize(10);
		ResultSet rs = st.executeQuery("select id,json_tweet from enai_tweet LIMIT 10000;");
		builder.buildFromResultSet(rs);
		c.setAutoCommit(true);
	}
	
	public ResultSet getTweets() throws SQLException {
		Statement st = c.createStatement();
		st.setFetchSize(10);
		return st.executeQuery("select tweet from tweets;");
	}
	
	public static void main(String[] args)
    {
		try {
			new TestTweetTable( EntityResolver.getGeonamesConnection() ).generate();
		} catch (Exception e) {
			System.out.println("#CAUGHT: "+e);
			e.printStackTrace();
		}
    }
}