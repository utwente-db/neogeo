/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis
 */
public abstract class ProcessPart {
    private static final Logger log = Logger.getLogger(ProcessPart.class);
    
    protected int rowCount;
    
    protected int medianOrder1 = -1;
    
    protected int medianOrder2 = -1;
    
    protected int partId;
    
    protected long startTime;
    
    protected long endTime;
    
    protected String sortedTable;
    
    public enum SHIFT_DIRECTION {
        Left, Right
    };
    
    protected Connection conn;
    
    protected Config config;
    
    protected abstract void createSortedTable()throws SQLException;
    
    protected abstract void generateDataStreamsFull () throws SQLException, IOException;
    protected abstract void insertIndexRecord () throws SQLException;
    
    protected abstract void generateDataStreamsShifting () throws SQLException, IOException;
    protected abstract void generateShifts () throws SQLException;
    protected abstract void insertMedian () throws SQLException;
    
    public void setConnection(Connection conn) {
        this.conn = conn;
    }

    public void setConfig(Config config) {
        this.config = config;
    }
    
    public void process (int partId, int startTime, int endTime) throws SQLException, IOException {
        this.process(partId, (long)startTime, (long)endTime);
    }
        
    public void process (int partId, long startTime, long endTime) throws SQLException, IOException {
        this.partId = partId;
        this.startTime = startTime;
        this.endTime = endTime;
        
        log.info("Processing part #" + partId + " (" + startTime + "-" + endTime + ") ...");
        
        // create sorted version of tuples
        createSortedTable();
        
        // count number of tuples in part
        doCount();
        
        if (config.getType() == Config.Type.SHIFTING) {
            this.processShifting();
        } else if (config.getType() == Config.Type.FULL) {
            this.processFull();
        } else {
            throw new UnsupportedOperationException("Unknown type " + config.getType());
        }
        
        log.info("Finished processing part #" + partId);
    }
    
    protected void processFull () throws SQLException, IOException {
        // generate data structures for full
        generateDataStreamsFull();
        
        // insert index record for this part
        insertIndexRecord();
    }
    
    protected void processShifting () throws SQLException, IOException {
        // determine median
        determineMedian();
        
        // generate data structures
        generateDataStreamsShifting();
        
        if (config.datastreamsOnly() == false) {
            // generate shift structures
            generateShifts();
        }
        
        // insert median info
        insertMedian();
    }
    
    protected void determineMedian () throws SQLException {
        log.info("Determining median information...");
        
        // even or odd number of tuples?        
        if (rowCount % 2 == 0) {
            // need to find two middle median tuples
            medianOrder1 = (rowCount / 2);
            medianOrder2 = (rowCount + 2) / 2;           
            
            log.info("Medians are located at #" + medianOrder1 + " and #" + medianOrder2);
        } else {
            // need to find single median tuple
            medianOrder1 = (rowCount + 1) / 2;
            
            log.info("Median is located at #" + medianOrder1);
        } 
    }
    
    protected void doCount () throws SQLException {
        log.info("Counting number of tuples...");
        PreparedStatement q = conn.prepareStatement("SELECT COUNT(*) AS row_count FROM " + sortedTable);
        
        ResultSet res = q.executeQuery();
        
        res.next();
        
        rowCount = res.getInt("row_count");
        
        res.close();
        q.close();
        
        log.info("Found " + Startup.formatter.format(rowCount) + " tuples");
    }
    
    
}
