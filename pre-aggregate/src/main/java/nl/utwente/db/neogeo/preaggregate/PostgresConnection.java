package nl.utwente.db.neogeo.preaggregate;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class PostgresConnection {
	private static final String CONFIG_FILENAME = "database.properties";

	private String hostname;
	private String port;
	private String username;
	private String password;
	private String database;

	public PostgresConnection(){
		readProperties(CONFIG_FILENAME);
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
	
	public static void main(String[] argv) {
		PostgresConnection psqlCon = new PostgresConnection();
		Connection con = psqlCon.getConnection();
		System.out.println("connection "+con.toString());
	}
}
