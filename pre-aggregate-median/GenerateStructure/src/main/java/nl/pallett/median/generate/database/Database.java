/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.database;

import java.sql.Connection;
import java.sql.SQLException;
import nl.pallett.median.generate.Config;

/**
 *
 * @author Dennis
 */
public abstract class Database {
    public abstract Connection openConnection (Config config) throws SQLException;
    public abstract void closeConnection (Connection conn) throws SQLException;    
}
