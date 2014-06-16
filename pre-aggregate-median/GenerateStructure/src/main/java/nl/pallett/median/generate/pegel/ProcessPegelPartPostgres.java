/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.pegel;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import nl.pallett.median.generate.Startup;
import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

/**
 *
 * @author Dennis
 */
public class ProcessPegelPartPostgres extends ProcessPegelPart {
    private static final Logger log = Logger.getLogger(ProcessPegelPartPostgres.class);
    
    @Override
    protected void createSortedTable () throws SQLException {
        log.info("Creating sorted table...");
        
        this.sortedTable = config.getTable() + "_level" + config.getSplitFactor() + "_part" + partId + "_sorted";
                
        Statement q = conn.createStatement();
        
        // check if table already exists
        q.execute("CREATE UNLOGGED TABLE IF NOT EXISTS " + sortedTable + " ( " +
                  "  order_id bigint," +
                  "  timed bigint NOT NULL," +
                  "  pegel double precision NOT NULL," +
                  "  CONSTRAINT " + sortedTable + "_pkey PRIMARY KEY (timed)" +
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
                    + " SELECT row_number() OVER (ORDER BY PEGEL ASC) AS order_id,"
                    + " timed, "
                    + " PEGEL "
                    + " FROM " + config.getTable()
                    + " WHERE timed >= " + startTime + " AND timed <= " + endTime
                    + " ORDER BY PEGEL ASC"
                    );

            log.info("Sorted data bas been inserted!");
        }
        
        q.close();
    }
        
    @Override
    protected void insertIndexRecord () throws SQLException {
        log.info("Inserting index record...");
        
        PreparedStatement q = conn.prepareStatement("INSERT INTO " + medianTable +
                                                    " (level, part, start_time, end_time, row_count)" +
                                                    " VALUES (?, ?, ?, ?, ?)");
        
        q.setInt(1, config.getSplitFactor());        
        q.setInt(2, partId);
        q.setLong(3, startTime);
        q.setLong(4, endTime);                   
        q.setInt(5, rowCount);
        
        q.execute();
        
        log.info("Index record inserted"); 
    }
    
    @Override
    protected void generateDataStreamsFull () throws SQLException, IOException {
        log.info("Generating data streams...");
        
        // select all sorted data from database
        PreparedStatement q = conn.prepareStatement("SELECT order_id, timed, pegel FROM " + sortedTable + " ORDER BY order_id ASC");
        ResultSet res = q.executeQuery();
        
        PreparedStatement insert = conn.prepareStatement("INSERT INTO " + dataTable + " (size, part, order_id, data) VALUES (?, ?, ?, ?)");
        int currSize = 0;
        int blockSize = config.getBlockSize();
        long[] data = new long[blockSize];
        int[] valueData = new int[blockSize];
        
        int dataOrderId = 1;        
        while(res.next()) {
            long timed = res.getLong("timed");
            double pegelDouble = res.getDouble("pegel");
            
            // convert pegel value to an integer, e.g. 355.23 -> 355230
            int pegel = (int)(pegelDouble * 1000);
            
            data[currSize] = timed;
            valueData[currSize] = pegel;
            currSize++; 
            
            if (currSize == blockSize) {
                // insert
                insertDataFull(insert, dataOrderId, currSize, data, valueData);
                dataOrderId++;
                
                data = new long[blockSize];
                valueData = new int[blockSize];
                currSize = 0;
            }
        }
        
         // insert final data stream
        if (currSize > 0) {
            insertDataFull(insert, dataOrderId, currSize, data, valueData);
            dataOrderId++;
        }       
        
        res.close();
        q.close();
        
        log.info("Finished generating data streams");    
    }
    
    protected void insertDataFull(PreparedStatement q, int dataOrderId, int size, long[] data, int[] valueData) throws SQLException, IOException {
        log.info("Inserting stream with size " + size + "...");
        
        q.setInt(1, size);
        q.setInt(2, this.partId);
        q.setInt(3, dataOrderId);
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        
        for(int i=0; i < data.length; i++) {
            long timed = data[i];
            int value = valueData[i];
            
            if (timed == 0) continue;
            
            dout.writeLong(timed);
            
            if (config.withValues()) {
                dout.writeInt(value);
            }
        }

        dout.close();
        byte[] asBytes = bout.toByteArray();
        
        q.setBytes(4, asBytes);
        
        q.execute();
        
        log.info("Stream inserted");
    }
    
    @Override
    protected void insertMedian () throws SQLException {
        log.info("Inserting median info...");
        
        PreparedStatement q = conn.prepareStatement("INSERT INTO " + medianTable +
                                                    " (level, part, start_time, end_time, median_time_1, median_value_1, median_time_2, median_value_2, row_count)" +
                                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");
        
        q.setInt(1, config.getSplitFactor());        
        q.setInt(2, partId);
        q.setLong(3, startTime);
        q.setLong(4, endTime);   
        
        q.setLong(5, medianTime1);
        q.setInt(6, medianValue1);   
        
        if (medianTime2 > -1) {
            q.setLong(7, medianTime2);
            q.setInt(8, medianValue2);
        } else {
            q.setNull(7, java.sql.Types.BIGINT);
            q.setNull(8, java.sql.Types.INTEGER);
        }

        q.setLong(9, rowCount);
        
        q.execute();
        
        log.info("Median info inserted");
    }
    
    @Override
    protected void insertShifts () throws SQLException {       
        log.info("Inserting shift data...");
                               
        PreparedStatement insert = conn.prepareStatement(
                "INSERT INTO " + shiftTable + " (timed, part, shift_left, shift_right) " +
                " SELECT l.timed, " + this.partId + ", l.shift AS shift_left, r.shift AS shift_right" + 
                " FROM " + shiftTable + "_left AS l" + 
                " INNER JOIN " + shiftTable + "_right AS r ON l.timed = r.timed" +
                " ORDER BY l.timed ASC"
        );
        
        insert.execute();      
        
        Statement q = conn.createStatement();
        
        // drop temporary tables
        q.execute("DROP TABLE " + shiftTable + "_left");
        q.execute("DROP TABLE " + shiftTable + "_right");
        
        log.info("Finished inserting shift data");
    }
    
    @Override
    protected void calculateLeftShifts () throws SQLException {
        log.info("Calculating left shifts...");
        
        PreparedStatement q = conn.prepareStatement("SELECT o.timed, s.order_id FROM " + config.getTable() + " AS o " +
                                                    " INNER JOIN " + sortedTable + " AS s " +
                                                    " ON o.timed = s.timed ORDER BY o.timed ASC, o.pk ASC");
                
        ResultSet res = q.executeQuery();
        
        leftShifts = new HashMap<Long, Integer>();        
        calculateShifts(res, "left");
        
        res.close();
        q.close();
        
        log.info("Finished calculating left shifts");      
    }
    
    @Override
    protected void calculateRightShifts () throws SQLException {
        log.info("Calculating right shifts...");
        
        PreparedStatement q = conn.prepareStatement("SELECT o.timed, s.order_id FROM " + config.getTable() + " AS o " +
                                                    " INNER JOIN " + sortedTable + " AS s " +
                                                    " ON o.timed = s.timed ORDER BY o.timed DESC, o.pk DESC");
                
        ResultSet res = q.executeQuery();
        
        rightShifts = new HashMap<Long, Integer>();        
        calculateShifts(res, "right");
        
        res.close();
        q.close();
        
        log.info("Finished calculating right shifts");      
    }
    
    protected void calculateShifts (ResultSet res, String dir) throws SQLException {
        int i = 0;
        int sumBelow = 0;
        int sumAbove = 0;
        int prevOrder = -1;
        
        String tempTableName = shiftTable + "_" + dir;
               
        Statement q = conn.createStatement();
        
        // drop any old version of table, if exists
        q.execute("DROP TABLE IF EXISTS " + tempTableName);
        
        // create new table
        q.execute("CREATE TEMP TABLE " + tempTableName + " ( " +
                     "  timed bigint NOT NULL," +
                     "  shift bigint NOT NULL," +
                     "  CONSTRAINT " + tempTableName + "_pkey PRIMARY KEY (timed)" +
                     ")"
                  );
        
        StringBuilder sb = new StringBuilder();
        CopyManager cpManager = ((PGConnection)conn).getCopyAPI();
        PushbackReader reader = new PushbackReader( new StringReader(""), 10000 );
        
        double compareOrder = medianOrder1;
        if (medianOrder2 != -1) {
            compareOrder = ((double)medianOrder1 + (double)medianOrder2) / 2;
        }     

        while(res.next()) {
            long timed = res.getLong("timed");
            int orderId = res.getInt("order_id");
            
            int shift = 0;
            
            if (prevOrder != -1) {
                if (prevOrder < compareOrder) {
                    sumBelow++;
                    shift = sumBelow - sumAbove;
                } else if (prevOrder > compareOrder) {
                    sumAbove++;
                    shift = sumBelow - sumAbove;
                } else if (prevOrder == compareOrder) {
                    shift = sumBelow - sumAbove;
                }
            }
            
            sb.append(timed).append(",")
              .append(shift).append("\n");
            
            i++;
            
            if (i % 200 == 0) {
                try {
                    reader.unread( sb.toString().toCharArray() );
                    cpManager.copyIn("COPY " + tempTableName + " FROM STDIN WITH CSV", reader );
                    sb.delete(0,sb.length());
                } catch (IOException e) {
                    throw new SQLException("Calculating shifts failed", e);
                }
            }
                        
            if (i % 100000 == 0) {
                log.info("Finished " + Startup.formatter.format(i) + " rows " + Math.floor(((double)i/(double)rowCount)*100) + "%");
            }
            
            prevOrder = orderId;
        }  
        
        try {
            reader.unread( sb.toString().toCharArray() );
            cpManager.copyIn("COPY " + tempTableName + " FROM STDIN WITH CSV", reader );
        } catch (IOException ex) {
            throw new SQLException("Calculating shifts failed", ex);
        }       
    }
    
    @Override
    protected void generateDataStreamsShifting () throws SQLException, IOException {
        log.info("Generating data streams...");
        
        // select all sorted data from database
        PreparedStatement q = conn.prepareStatement("SELECT order_id, timed, pegel FROM " + sortedTable + " ORDER BY order_id ASC");
        ResultSet res = q.executeQuery();
        
        int dir = -1;
        int blockSize = config.getBlockSize();
        
        PreparedStatement insert = conn.prepareStatement("INSERT INTO " + dataTable + " (direction, size, part, data) VALUES (?, ?, ?, ?)");
        int currSize = 0;
        long[] data = new long[blockSize];
        int[] valueData = new int[blockSize];
        while(res.next()) {
            long order_id = res.getLong("order_id");
            long timed = res.getLong("timed");
            double pegelDouble = res.getDouble("pegel");
            
            // convert pegel value to an integer, e.g. 355.23 -> 355230
            int pegel = (int)(pegelDouble * 1000);
                       
            if (order_id == medianOrder1) {
                medianTime1 = timed;
                medianValue1 = pegel;
                
                // insert current data stream
                insertDataShifting(insert, dir, currSize, data, valueData);
                
                data = new long[blockSize];
                valueData = new int[blockSize];
                currSize = 0;
                
                // switch direction from left to right
                dir = 1;
                
                continue;
            }
            
            if (order_id == medianOrder2) {
                medianTime2 = timed;
                medianValue2 = pegel;
                continue;
            }  
                       
            data[currSize] = timed;
            valueData[currSize] = pegel;
            currSize++;            
            
            if (currSize == blockSize) {
                // insert
                insertDataShifting(insert, dir, currSize, data, valueData);
                
                data = new long[blockSize];
                valueData = new int[blockSize];
                currSize = 0;
            }
        }
        
        // insert final data stream
        if (currSize > 0) {
            insertDataShifting(insert, dir, currSize, data, valueData);
        }       
        
        res.close();
        q.close();
        
        log.info("Finished generating data streams");        
    }
    
    protected void insertDataShifting(PreparedStatement q, int dir, int size, long[] data, int[] valueData) throws SQLException, IOException {
        log.info("Inserting stream for direction " + dir + " with size " + size + "...");
        
        q.setInt(1, dir);
        q.setInt(2, size);
        q.setInt(3, this.partId);
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        
        // if direction is below median (-1) then write data elements in reverse order
        // this makes it easier to use during median selection
        if (dir == -1) {
            for(int i=data.length; i > 0; i--) {
                long timed = data[i-1];
                if (timed == 0) continue;
                
                dout.writeLong(timed);
                
                if (config.withValues()) {
                    dout.writeInt(valueData[i-1]);
                }
            }            
        } else {
            for(int i=0; i < data.length; i++) {
                long timed = data[i];
                if (timed == 0) continue;
                
                dout.writeLong(timed);
                
                if (config.withValues()) {
                    dout.writeInt(valueData[i]);
                }
            }
        }

        dout.close();
        byte[] asBytes = bout.toByteArray();
        
        q.setBytes(4, asBytes);
        
        q.execute();
        
        log.info("Stream inserted");
    }
    
}
