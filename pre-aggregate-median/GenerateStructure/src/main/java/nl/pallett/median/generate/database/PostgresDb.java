/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import nl.pallett.median.generate.Config;
import nl.pallett.median.generate.pegel.GeneratePegelStructurePostgres;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis
 */
public class PostgresDb extends Database {
    private static final Logger log = Logger.getLogger(PostgresDb.class);
    
    @Override
    public Connection openConnection (Config config) throws SQLException {
        // load driver class
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            throw new SQLException("JDBC Driver cannot be initialized");
        }

        String url = "jdbc:postgresql://" + config.getHost() + ":" + config.getPort() + "/" + config.getDatabase();
        Properties props = new Properties();
        props.setProperty("user", config.getUser());
        props.setProperty("password", config.getPassword());
        
        log.info("Opening database connection with " + url + " ...");

        Connection conn = DriverManager.getConnection(url, props);
        
        log.info("Database connection opened!");
        return conn;
    }
    
    @Override
    public void closeConnection (Connection conn) {
        
    }
}
