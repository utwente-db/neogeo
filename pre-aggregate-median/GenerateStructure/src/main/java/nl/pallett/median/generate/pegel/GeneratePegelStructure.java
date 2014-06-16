/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.pegel;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import nl.pallett.median.generate.Config;
import nl.pallett.median.generate.GenerateStructure;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis
 */
public abstract class GeneratePegelStructure extends GenerateStructure {
    private static final Logger log = Logger.getLogger(GeneratePegelStructure.class);
    
    protected Connection conn;
    
    protected String dataTable;
    
    protected String shiftTable;
    
    protected String medianTable;
        
    protected abstract void prepareTablesFull() throws SQLException;
    
    protected abstract void prepareTablesShifting() throws SQLException;
    
    protected abstract void finalizeTables() throws SQLException;
    
    protected abstract long[] getMinMaxTime () throws SQLException;
    
    protected abstract ProcessPegelPart createProcessPartObject ();
    
    @Override
    public void run () throws SQLException, IOException {
        log.info("Creating structure for PEGEL dataset for database " + config.getDatabaseType());
        
        // open DB connection
        this.conn = database.openConnection(this.config);
        
        // prepare tables
        prepareTables();
        
        // determine list of parts
        ArrayList<long[]> partList = determineParts();
        log.info("Found " + partList.size() + " parts");
        
        // process each part
        for(int i=0; i < partList.size(); i++) {
            long[] part = partList.get(i);          
            
            ProcessPegelPart process = createProcessPartObject();
            process.setConnection(conn);
            process.setConfig(config);
            process.setTables(dataTable, shiftTable, medianTable);
            
            process.process(i+1, part[0], part[1]);            
        }   
        
        // finalize tables
        finalizeTables();

        // close DB connection
        database.closeConnection(conn);
        
        
        log.info("Finished structure for PEGEL dataset");
    }
    
    protected void prepareTables () throws SQLException {
        if (config.getType() == Config.Type.FULL) {
            prepareTablesFull();
        } else {
            prepareTablesShifting();
        }
    }
    
    protected ArrayList<long[]> determineParts () throws SQLException {
        log.info("Determining parts...");

        long[] minMaxTime = this.getMinMaxTime();
        long minTime = minMaxTime[0];
        long maxTime = minMaxTime[1];        
        
        long interval = maxTime - minTime;
        long step = interval / config.getSplitFactor();
        
        ArrayList<long[]> partList = new ArrayList<long[]>();
        for(int i=0; i < config.getSplitFactor(); i++) {
            maxTime = minTime + step;
            
            long[] part = {minTime, maxTime};
            partList.add(part);
            
            minTime = maxTime;
        }
        
        return partList;
    }
    
}
