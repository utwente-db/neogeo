/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.twitter;

import java.io.IOException;
import java.sql.SQLException;
import nl.pallett.median.generate.Config;
import nl.pallett.median.generate.ProcessPart;
import org.apache.log4j.Logger;

/**
 *
 * @author
 * Dennis
 */
public abstract class ProcessTwitterPart extends ProcessPart {
    private static final Logger log = Logger.getLogger(ProcessTwitterPart.class);
        
    protected String dataTable;
        
    protected String medianTable;
        
    public void setTables (String dataTable, String medianTable) {
        this.dataTable = dataTable;
        this.medianTable = medianTable;
    }

    

}
