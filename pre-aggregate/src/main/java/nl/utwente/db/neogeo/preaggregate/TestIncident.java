package nl.utwente.db.neogeo.preaggregate;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

public class TestIncident {
	private static final String CONFIG_FILENAME = "nspyer_database.properties";
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
		TestIncident t = new TestIncident();
		t.readProperties();
		Connection connection = t.getConnection();
		//runTest( connection );
		setup_nspyre( connection );
	}

	public static void setup_nspyre(Connection c) throws Exception {
		try {
			System.out.println("Setting up nspyre again!");
			GeotaggedIncidentAggregate pa = new GeotaggedIncidentAggregate(c, "public", "raw_coord_geoserver_agg", null, "sos_aggregate_time", "coord",2 /* axis 2 split*/,2000,null);
			c.close();
		} catch (SQLException e) {
			System.out.println("Caught: " + e);
			e.printStackTrace(System.out);
		}
		System.out.println("#!finished");
	}
	

	
}
