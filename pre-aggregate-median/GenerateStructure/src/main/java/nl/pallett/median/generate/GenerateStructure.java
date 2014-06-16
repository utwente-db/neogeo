/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate;

import java.io.IOException;
import java.sql.SQLException;
import nl.pallett.median.generate.database.Database;

/**
 *
 * @author Dennis
 */
public abstract class GenerateStructure {
    protected Config config;
    
    protected Database database;
    
    public void setConfig(Config config) {
        this.config = config;
    }
    
    public void setDatabase(Database database) {
        this.database = database;
    }
    
    public abstract void run ()  throws SQLException, IOException;
    
}
