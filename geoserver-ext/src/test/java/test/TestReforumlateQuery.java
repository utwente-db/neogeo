package org.geotools.data.aggregation.test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

import nl.utwente.db.neogeo.preaggregate.AggregateAxis;
import nl.utwente.db.neogeo.preaggregate.AxisSplitDimension;
import nl.utwente.db.neogeo.preaggregate.GeotaggedTweetAggregate;

import org.geotools.data.aggregation.Area;
import org.geotools.data.aggregation.PreAggregate;

public class TestReforumlateQuery {
	private static final String CONFIG_FILENAME = "database_postgres.properties";
	private static PreAggregate agg;
	private static int[] iv_count = {10, 10, 1};
	private String hostname;
	private String port;
	private String username;
	private String password;
	private String database;

	private void readProperties() {
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Connection getConnection(){
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return null;
		}
		System.out.println("PostgreSQL JDBC Driver Registered!");
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(
					"jdbc:postgresql://"+hostname+":"+port+"/"+database, username, password);
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;

		}
		if (connection != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}
		return connection;
	}
	
	public static void main(String[] argv) throws Exception {
		System.out.println("Test pre-aggregate package");
		TestReforumlateQuery t = new TestReforumlateQuery();
		t.readProperties();
		Connection connection = t.getConnection();
		runTest( connection );
	}

	public static void runTest(Connection c) throws Exception {
		try {
			 Area area = new Area(-0.11900000000000001,0.448,51.328,51.658);
			 
			agg = new PreAggregate(c, "public", "london_hav_neogeo", "myAggregate"); 
			System.out.println("\n\n with splitting!");
			int i=0;
//			for(AggregateAxis a : agg.getAxis()){
//				if(a.columnExpression().startsWith("ST_X")){
//					AxisSplitDimension dim = a.splitAxis(-0.11900000000000001,0.448,10);
//					if(dim==null) throw new Exception("query area out of available data domain");
//				}
//			}
//			 startTime:2011-10-11 15:00:00.0    endTime:2011-10-30 13:00:00.0
			 reformulateQuery(area, null, null);
		} catch (SQLException e) {
			System.out.println("Caught: " + e);
			e.printStackTrace(System.out);
		}
		System.out.println("#!finished");
	}

	private static Object[][] reformulateQuery(Area area, Timestamp start, Timestamp end) throws Exception{
		int i=0;
		//int[] range = new int[agg.getAxis().length];
		Object[][] iv_obj = new Object[agg.getAxis().length][3];
		for(AggregateAxis a : agg.getAxis()){
			AxisSplitDimension dim = null;
			if(a.columnExpression().startsWith("ST_X")) {
				dim = a.splitAxis(area.getLowX(), area.getHighX(), iv_count [0]);
				i=0;
			}
			if(a.columnExpression().startsWith("ST_Y")) {
				dim = a.splitAxis(area.getLowY(), area.getHighY(), iv_count[1]);
				i=1;
			}
			if(false){
//			if(a==time) {
				dim = a.splitAxis(start, end, iv_count[2]);
				i=2;
			}
			if(dim==null) throw new Exception("query area out of available data domain due to problems in axis "+a.columnExpression());
			//			range[i] = dim.getCount();
//			LOGGER.severe("dim values:"+dim.toString());
			iv_obj[i][0] = dim.getStart();
			iv_obj[i][1] = dim.getEnd();
			iv_obj[i][2] = dim.getCount();
		}
		//		ResultSet rs = agg.SQLquery_grid(PreAggregate.AGGR_COUNT, iv_first_obj, range);
		//		return rs;
		return iv_obj;
	}
}
