package nl.utwente.db.neogeo.preaggregate.ui;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class DbInfo {
    
    protected String hostname;
    
    protected int port;
    
    protected String username;
    
    protected String password;
    
    protected String database;
    
    protected String schema = "public";
    
    protected String driverClass = "org.postgresql.Driver";
    
    protected String urlPrefix = "jdbc:postgresql";
    
    public DbInfo (String propsFilePath) throws IOException {
        loadFile(propsFilePath);
    }
    
    public void loadFile (String propsFilePath) throws IOException {
        File propsFile = new File(propsFilePath);
        
        if (propsFile.exists() == false) {
            throw new IllegalArgumentException("Invalid database.properties specified; file path '" + propsFilePath + "' does not exist");
        }
        
        Properties prop = new Properties();
        prop.load(new FileInputStream(propsFile));
        
        load(prop);
    }
    
    public void load (Properties prop) {
        hostname = prop.getProperty("hostname");
        port = Integer.parseInt(prop.getProperty("port"));
        username = prop.getProperty("username");
        password = prop.getProperty("password");
        database = prop.getProperty("database");
        
        if (prop.containsKey("schema")) {
            schema = prop.getProperty("schema");
        }

        if (prop.containsKey("driver")) {
            driverClass = prop.getProperty("driver");
        }

        if (prop.containsKey("url_prefix")) {
            urlPrefix = prop.getProperty("url_prefix");
        }
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        return schema;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public String getUrlPrefix() {
        return urlPrefix;
    }
    
}
