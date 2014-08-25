package nl.utwente.db.neogeo.preaggregate;


import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MysqlTest {
	//private static final String CONFIG_FILENAME = "database.properties";
	private static final String CONFIG_FILENAME = "database_postgres.properties";

	public static final double	DFLT_BASEBOXSIZE = 0.001;
	public static final short	DFLT_N = 4;

	private static final String point_column = "coordinates";
	
	
	private String hostname;
	private String port;
	private String username;
	private String password;
	private String database;

	public MysqlTest(){
		readProperties(CONFIG_FILENAME);
	}
	
	public Connection getConnection(){
		try {
			Class.forName("org.postgresql.Driver");
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your Mysql JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return null;
		}
		System.out.println("MySQL JDBC Driver Registered!");
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(
//					"jdbc:mysql://"+hostname+":"+port+"/"+database, username, password);
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] argv) throws Exception {
		MysqlTest psqlCon = new MysqlTest();
		Connection con = psqlCon.getConnection();
		System.out.println("connection "+con.toString());
		//psqlCon.readPreAggregateTest(con);
		Test test = new Test();
		test.runTest(con, "public");
		// to create the pre-aggregate
		//PegelAndelfingen2Aggregate pegel = new PegelAndelfingen2Aggregate(con, "datagraph" , "pegel_andelfingen2", "andelfingen2", "timed");
		// to operate the pre-aggregate
//		PegelAndelfingen2Aggregate pegel = new PegelAndelfingen2Aggregate(c, "public" , "andelfingen2", "pegel_andelfingen2", "timed");
		// PegelAndelfingen2Aggregate pegel = new PegelAndelfingen2Aggregate(c, "public" , "andelfingen2", "pegel_andelfingen2");
//		pegel.timeQuery("count", 1167606600, 1312737480);

//		PegelAndelfingen2Aggregate pegel = new PegelAndelfingen2Aggregate(con, "datagraph" , "pegel_andelfingen2", "andelfingen2", "timed");
//		pegel.timeQuery("count", 1167606600, 1312737480);
	}

	private void readPreAggregateTest(Connection c) {
		try {
			PreAggregate agg = new PreAggregate(c, "public", "andelfingen2", "pegel_andelfingen2");
//			AggregateAxis[] axis = agg.getAxis();
//			Object[][] obj_range = agg.getRangeValues(c, "public", "andelfingen2", axis);
			Object[][] obj_range = agg.getRangeValues(c);
			agg.SQLquery(PreAggregate.AGGR_COUNT, obj_range);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("result");
	}
}