package nl.utwente.db.named_entity_recog;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * @author badiehm & flokstra
 */
public class GeoNamesDB
{
	public static final boolean verbose = true;
	
	private static final String CONFIG_FILENAME = "database.properties";
	
	public static Connection getConnection()
    {
        String hostname = null;
        String port = null;
        String username = null;
        String password = null;
        String database = null;

        Properties prop = new Properties();
        try
        {
            InputStream is =(new GeoNamesDB()).getClass().getClassLoader().getResourceAsStream(CONFIG_FILENAME);
            prop.load(is);
            hostname = prop.getProperty("hostname");
            port = prop.getProperty("port");
            username = prop.getProperty("username");
            password = prop.getProperty("password");
            database = prop.getProperty("database");
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try
        {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e)
        {
            System.out.println("Where is your PostgreSQL JDBC Driver? "
                    + "Include in your library path!");
            e.printStackTrace();
            return null;
        }
        if (verbose)
        {
            System.out.println("PostgreSQL JDBC Driver Registered!");
        }
        Connection connection = null;
        try
        {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://" + hostname + ":" + port + "/" + database, username, password);
        }
        catch (SQLException e)
        {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return null;

        }
        if (connection != null)
        {
            if (verbose)
            {
                System.out.println("Connected to jdbc:postgresql://" + hostname + ":" + port + "/" + database + " as \"" + username + "\"");

            }
        }
        else
        {
            throw new RuntimeException("Failed to make connection!");
        }
        return connection;
    }
    // connection should also be visible in other packages
    public static Connection geonames_conn = getConnection();

    public void refreshConnection() throws SQLException
    {
        geonames_conn = getConnection();
    }

}
