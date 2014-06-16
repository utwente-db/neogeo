/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.twitter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

/**
 *
 * @author
 * Dennis
 */
public class ProcessTwitterPartPostgres extends ProcessTwitterPart {
    private static final Logger log = Logger.getLogger(ProcessTwitterPartPostgres.class);

    @Override
    protected void createSortedTable() throws SQLException {
        log.info("Creating sorted table...");
        
        this.sortedTable = config.getTable() + "_level" + config.getSplitFactor() + "_part" + partId + "_sorted";
                
        Statement q = conn.createStatement();
        
        // check if table already exists
        q.execute("CREATE UNLOGGED TABLE IF NOT EXISTS " + sortedTable + " ( " +
                  "  order_id bigint NOT NULL," +
                  "  row_id bigint NOT NULL," +
                  "  timed int NOT NULL," +
                  "  x_coordinate double precision NOT NULL," +
                  "  y_coordinate double precision NOT NULL," +
                  "  len smallint, " +
                  "  CONSTRAINT " + sortedTable + "_pkey PRIMARY KEY (order_id)" +
                  ")");        
       
        log.info("Table created!");
        
        // check if table already has data
        ResultSet res = q.executeQuery("SELECT * FROM " + sortedTable + " LIMIT 1");
        boolean hasData = res.next();
        res.close();
        
        if (hasData) {
            log.info("Data already found in sorted table; not re-inserting new data");
        } else {        
            log.info("Sorting and inserting data into sorted table... this may take a while!");
        
            q.execute("INSERT INTO " + sortedTable + " "
                    + " SELECT row_number() OVER (ORDER BY len ASC) AS order_id,"
                    + " id,"
                    + " timed,"
                    + " x_coordinate,"
                    + " y_coordinate,"
                    + " len"
                    + " FROM " + config.getTable()
                    + " WHERE timed >= " + startTime + " AND timed <= " + endTime
                    + " ORDER BY len ASC"
                    );
            
            q.execute("VACUUM ANALYZE " + sortedTable);

            log.info("Sorted data bas been inserted!");
        }
        
        q.close();
    }

    @Override
    protected void generateDataStreamsFull() throws SQLException, IOException {
        log.info("Generating data streams...");
        
        // select all sorted data from database
        PreparedStatement q = conn.prepareStatement("SELECT row_id, timed, x_coordinate, y_coordinate, len FROM " + sortedTable + " ORDER BY order_id ASC");
        ResultSet res = q.executeQuery();
        
        PreparedStatement insert = conn.prepareStatement("INSERT INTO " + dataTable + " (size, part, order_id, data) VALUES (?, ?, ?, ?)");
        int currSize = 0;
        int blockSize = config.getBlockSize();
        
        long[] idData = new long[blockSize];
        int[] timedData = new int[blockSize];
        double[] xCoordinateData = new double[blockSize];
        double[] yCoordinateData = new double[blockSize];
        short[] valueData =  new short[blockSize];
        
        int dataOrderId = 1;        
        while(res.next()) {
            timedData[currSize] = res.getInt("timed");
            xCoordinateData[currSize] = res.getDouble("x_coordinate");
            yCoordinateData[currSize] = res.getDouble("y_coordinate");
            
            if (config.withValues()) {
                valueData[currSize] = res.getShort("len");
            } else {
                idData[currSize] = res.getLong("row_id");
            }
                
            currSize++; 
            
            if (currSize == blockSize) {
                // insert
                insertDataFull(insert, dataOrderId, currSize, idData, timedData, xCoordinateData, yCoordinateData, valueData);
                dataOrderId++;
                
                idData = new long[blockSize];
                timedData = new int[blockSize];
                xCoordinateData = new double[blockSize];
                yCoordinateData = new double[blockSize];
                valueData =  new short[blockSize];
                currSize = 0;
            }
        }
        
         // insert final data stream
        if (currSize > 0) {
            insertDataFull(insert, dataOrderId, currSize, idData, timedData, xCoordinateData, yCoordinateData, valueData);
            dataOrderId++;
        }       
        
        res.close();
        q.close();
        
        log.info("Finished generating data streams");
    }
    
    protected void insertDataFull(PreparedStatement q, int dataOrderId, int size, long[] idData, 
            int[] timedData, double[] xCoordinateData, double[] yCoordinateData, short[] valueData) throws SQLException, IOException {
        log.info("Inserting stream with size " + size + "...");
        
        q.setInt(1, size);
        q.setInt(2, this.partId);
        q.setInt(3, dataOrderId);
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        
        for(int i=0; i < idData.length; i++) {
            if (config.withValues() == false) {
                dout.writeLong(idData[i]);
            }
            
            dout.writeInt(timedData[i]);
            dout.writeDouble(xCoordinateData[i]);
            dout.writeDouble(yCoordinateData[i]);
            
            if (config.withValues()) {
                dout.writeShort(valueData[i]);
            }
        }

        dout.close();
        byte[] asBytes = bout.toByteArray();
        
        q.setBytes(4, asBytes);
        
        q.execute();
        
        log.info("Stream inserted");
    }

    @Override
    protected void insertIndexRecord() throws SQLException {
        log.info("Inserting index record...");
        
        PreparedStatement q = conn.prepareStatement("INSERT INTO " + medianTable +
                                                    " (level, part, start_time, end_time, row_count)" +
                                                    " VALUES (?, ?, ?, ?, ?)");
        
        q.setInt(1, config.getSplitFactor());        
        q.setInt(2, partId);
        q.setInt(3, (int)startTime);
        q.setInt(4, (int)endTime);                   
        q.setInt(5, rowCount);
        
        q.execute();
        
        log.info("Index record inserted"); 
    }

    @Override
    protected void generateDataStreamsShifting() throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void generateShifts() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected void insertMedian() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
