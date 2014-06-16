/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.knmi;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import nl.pallett.median.generate.Config;
import nl.pallett.median.generate.GenerateStructure;
import nl.pallett.median.generate.pegel.ProcessPegelPart;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis
 */
public abstract class GenerateKnmiStructure extends GenerateStructure {
    private static final Logger log = Logger.getLogger(GenerateKnmiStructure.class);
    
    protected Connection conn;
    
    protected String dataTable;
    
    protected String timeShiftTable;
    
    protected String stationShiftTable;
        
    protected String medianTable;

    @Override
    public void run() throws SQLException, IOException {
        log.info("Creating structure for KNMI dataset for database " + config.getDatabaseType());
        
        // open DB connection
        this.conn = database.openConnection(this.config);
        
        // prepare tables
        prepareTables();
        
         // determine list of parts
        ArrayList<int[]> partList = determineParts();
        log.info("Found " + partList.size() + " parts");
        
        // process each part
        for(int i=0; i < partList.size(); i++) {
            int[] part = partList.get(i);          
            
            ProcessKnmiPart process = createProcessPartObject();
            process.setConnection(conn);
            process.setConfig(config);
            process.setTables(dataTable, stationShiftTable, timeShiftTable, medianTable);
                                    
            process.process(i+1, part[0], part[1]);            
        }   
        
        
        // finalize tables
        finalizeTables();
        
        database.closeConnection(this.conn);        
        
        log.info("Finished structure for KNMI dataset");
    }
    
    protected void prepareTables () throws SQLException {
        if (config.getType() == Config.Type.FULL) {
            prepareTablesFull();
        } else {
            prepareTablesShifting();
        }
    }
    
    protected ArrayList<int[]> determineParts () throws SQLException {
        log.info("Determining parts...");

        int[] minMaxTime = this.getMinMaxTime();
        int minTime = minMaxTime[0];
        int maxTime = minMaxTime[1];        
        
        int interval = maxTime - minTime;
        int step = interval / config.getSplitFactor();
        
        ArrayList<int[]> partList = new ArrayList<int[]>();
        for(int i=0; i < config.getSplitFactor(); i++) {
            maxTime = minTime + step;
            
            int[] part = {minTime, maxTime};
            partList.add(part);
            
            minTime = maxTime;
        }
        
        return partList;
    }
    
    protected abstract void prepareTablesFull() throws SQLException;    
    protected abstract void prepareTablesShifting() throws SQLException;
    protected abstract void finalizeTables() throws SQLException;
    protected abstract int[] getMinMaxTime () throws SQLException;
    protected abstract ProcessKnmiPart createProcessPartObject ();
    
}
