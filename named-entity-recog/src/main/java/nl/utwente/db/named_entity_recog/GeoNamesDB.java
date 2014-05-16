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
	public static final boolean verbose = false;
	
	private static final String CONFIG_FILENAME = "database.properties";
	
	private static Connection createConnection() throws SQLException 
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
            throw new SQLException(""+e);
        }
        try
        {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e)
        {
        	e.printStackTrace();
            throw new SQLException("Where is your PostgreSQL JDBC Driver? "
                    + "Include in your library path!");
        }
        if (verbose)
        {
            System.out.println("PostgreSQL JDBC Driver Registered!");
        }
        Connection connection = null;
        
        connection = DriverManager.getConnection(
                    "jdbc:postgresql://" + hostname + ":" + port + "/" + database, username, password);
      
        if (connection != null)
        {
            if (verbose)
            {
                System.out.println("Connected to jdbc:postgresql://" + hostname + ":" + port + "/" + database + " as \"" + username + "\"");

            }
        }
        else
        {
            throw new SQLException("Failed to make connection!");
        }
        return connection;
    }
	
    private static Connection geonames_conn = null;

    public static void discardConnection()
    {
        geonames_conn = null;
    }
    
    public static Connection geoNameDBConnection() throws SQLException {
    	if ( geonames_conn == null )
    		geonames_conn = createConnection();
    	return geonames_conn;
    }

}
