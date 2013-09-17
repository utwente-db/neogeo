package nl.utwente.db.neogeo.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DbConnection {
	
	private static final String CONFIG_FILENAME = "database.properties";
	
	private static final boolean verbose = true;
	
	private String hostname;
	private String port;
	private String username;
	private String password;
	private String database;

	DbConnection() {
		readProperties();
	}
	
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
			if ( verbose )
				System.out.println("DbConnection: properties to db "+hostname+":"+database+" read.");
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
		if ( verbose )
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
			if ( verbose )
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("#!DbConnection: Failed to make connection!");
		}
		return connection;
	}

	public static final DbConnection connector = new DbConnection();
	
	public static void main(String[] argv) throws Exception {
		System.out.println("Test connection");
		
		Connection connection = connector.getConnection();
	}
	
}
