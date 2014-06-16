/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.knmi;

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
public class ProcessKnmiPartPostgres extends ProcessKnmiPart {
    private static final Logger log = Logger.getLogger(ProcessKnmiPartPostgres.class);
    
    @Override
    protected void createSortedTable () throws SQLException {
        log.info("Creating sorted table...");
        
        this.sortedTable = config.getTable() + "_level" + config.getSplitFactor() + "_part" + partId + "_sorted";
                
        Statement q = conn.createStatement();
        
        // check if table already exists
        q.execute("CREATE UNLOGGED TABLE IF NOT EXISTS " + sortedTable + " ( " +
                  "  order_id bigint NOT NULL," +
                  "  row_id int NOT NULL," +
                  "  timed int NOT NULL," +
                  "  station int NOT NULL," +
                  "  temperature smallint, " +
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
                    + " SELECT row_number() OVER (ORDER BY temperature ASC) AS order_id,"
                    + " id,"
                    + " timed,"
                    + " station,"
                    + " temperature"
                    + " FROM " + config.getTable()
                    + " WHERE timed >= " + startTime + " AND timed <= " + endTime
                    + " ORDER BY temperature ASC"
                    );
            
            q.execute("VACUUM ANALYZE " + sortedTable);

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
        q.setInt(3, (int)startTime);
        q.setInt(4, (int)endTime);                   
        q.setInt(5, rowCount);
        
        q.execute();
        
        log.info("Index record inserted"); 
    }
    
    @Override
    protected void generateDataStreamsFull () throws SQLException, IOException {
        log.info("Generating data streams...");
        
        // select all sorted data from database
        PreparedStatement q = conn.prepareStatement("SELECT row_id, timed, station, temperature FROM " + sortedTable + " ORDER BY order_id ASC");
        ResultSet res = q.executeQuery();
        
        PreparedStatement insert = conn.prepareStatement("INSERT INTO " + dataTable + " (size, part, order_id, data) VALUES (?, ?, ?, ?)");
        int currSize = 0;
        int blockSize = config.getBlockSize();
        
        int[] idData = new int[blockSize];
        int[] timedData = new int[blockSize];
        byte[] stationData = new byte[blockSize];
        short[] valueData =  new short[blockSize];
        
        int dataOrderId = 1;        
        while(res.next()) {
            timedData[currSize] = res.getInt("timed");
            stationData[currSize] = res.getByte("station");
            
            if (config.withValues()) {
                valueData[currSize] = res.getShort("temperature");
            } else {
                idData[currSize] = res.getInt("row_id");
            }
                
            currSize++; 
            
            if (currSize == blockSize) {
                // insert
                insertDataFull(insert, dataOrderId, currSize, idData, timedData, stationData, valueData);
                dataOrderId++;
                
                idData = new int[blockSize];
                timedData = new int[blockSize];
                stationData = new byte[blockSize];
                valueData =  new short[blockSize];
                currSize = 0;
            }
        }
        
         // insert final data stream
        if (currSize > 0) {
            insertDataFull(insert, dataOrderId, currSize, idData, timedData, stationData, valueData);
            dataOrderId++;
        }       
        
        res.close();
        q.close();
        
        log.info("Finished generating data streams");    
    }
    
    protected void insertDataFull(PreparedStatement q, int dataOrderId, int size, int[] idData, 
            int[] timedData, byte[] stationData, short[] valueData) throws SQLException, IOException {
        log.info("Inserting stream with size " + size + "...");
        
        q.setInt(1, size);
        q.setInt(2, this.partId);
        q.setInt(3, dataOrderId);
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        
        for(int i=0; i < idData.length; i++) {
            if (config.withValues() == false) {
                dout.writeInt(idData[i]);
            }
            
            dout.writeInt(timedData[i]);
            dout.writeByte(stationData[i]);
            
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
    protected void generateDataStreamsShifting() throws SQLException, IOException {
        log.info("Generating data streams...");
        
        // select all sorted data from database
        PreparedStatement q = conn.prepareStatement("SELECT row_id, station, order_id, timed, temperature FROM " + sortedTable + " ORDER BY order_id ASC");
        ResultSet res = q.executeQuery();
        
        int dir = -1;
        int blockSize = config.getBlockSize();
        
        PreparedStatement insert = conn.prepareStatement("INSERT INTO " + dataTable + " (direction, size, part, data) VALUES (?, ?, ?, ?)");
        int currSize = 0;
        
        int[] idData = new int[blockSize];
        int[] timedData = new int[blockSize];
        byte[] stationData = new byte[blockSize];
        short[] valueData = new short[blockSize];
        
        while(res.next()) {
            int order_id = res.getInt("order_id");
            int timed = res.getInt("timed");
            int rowId = res.getInt("row_id");
            byte station = res.getByte("station");
            short temperature = res.getShort("temperature");
                       
            if (order_id == medianOrder1) {
                medianRowId1 = rowId;
                medianTime1 = timed;
                medianStation1 = station;
                medianTemperature1 = temperature;
                
                // insert current data stream
                insertDataShifting(insert, dir, currSize, idData, timedData, stationData, valueData);
                
                idData = new int[blockSize];
                timedData = new int[blockSize];
                stationData = new byte[blockSize];
                valueData = new short[blockSize];
                currSize = 0;
                
                // switch direction from left to right
                dir = 1;
                
                continue;
            }
            
            if (order_id == medianOrder2) {
                medianRowId2 = rowId;
                medianTime2 = timed;
                medianStation2 = station;
                medianTemperature2 = temperature;
                continue;
            }  
                       
            idData[currSize] = rowId;
            timedData[currSize] = timed;
            stationData[currSize] = station;
            valueData[currSize] = temperature;
            currSize++;            
            
            if (currSize == blockSize) {
                // insert
                insertDataShifting(insert, dir, currSize, idData, timedData, stationData, valueData);
                
                idData = new int[blockSize];
                timedData = new int[blockSize];
                stationData = new byte[blockSize];
                valueData = new short[blockSize];
                currSize = 0;
            }
        }
        
        // insert final data stream
        if (currSize > 0) {
            insertDataShifting(insert, dir, currSize, idData, timedData, stationData, valueData);
        }       
        
        res.close();
        q.close();
        
        log.info("Finished generating data streams");  
    }
    
    protected void insertDataShifting(PreparedStatement q, int direction, int size, int[] idData,
            int[] timedData, byte[] stationData, short[] valueData) throws SQLException, IOException {
        log.info("Inserting stream with size " + size + "...");
        
        q.setInt(1, direction);
        q.setInt(2, size);
        q.setInt(3, this.partId);
        
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        
        // if direction is below median (-1) then write data elements in reverse order
        // this makes it easier to use during median selection
        if (direction == -1) {
            for(int i = idData.length; i > 0; i--) {
                if (idData[i-1] == 0) continue;
                
                if (config.withValues() == false) {
                    dout.writeInt(idData[i-1]);
                }

                dout.writeInt(timedData[i-1]);
                dout.writeByte(stationData[i-1]);

                if (config.withValues()) {
                    dout.writeShort(valueData[i-1]);
                }
            }             
        } else {        
            for(int i=0; i < idData.length; i++) {
                if (config.withValues() == false) {
                    dout.writeInt(idData[i]);
                }

                dout.writeInt(timedData[i]);
                dout.writeByte(stationData[i]);

                if (config.withValues()) {
                    dout.writeShort(valueData[i]);
                }
            }
        }

        dout.close();
        byte[] asBytes = bout.toByteArray();
        
        q.setBytes(4, asBytes);
        
        q.execute();
        
        log.info("Stream inserted");
    }

    @Override
    protected void insertMedian() throws SQLException {
        log.info("Inserting median info...");
        
        PreparedStatement q = conn.prepareStatement("INSERT INTO " + medianTable +
                                                    " (level, part, start_time, end_time, median_rowid_1, median_time_1, median_station_1, median_temperature_1," +
                                                    "median_rowid_2, median_time_2, median_station_2, median_temperature_2,  row_count)" +
                                                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        
        q.setInt(1, config.getSplitFactor());        
        q.setInt(2, partId);
        q.setInt(3, (int)startTime);
        q.setInt(4, (int)endTime);   
        
        q.setInt(5, medianRowId1);
        q.setInt(6, medianTime1);
        q.setInt(7, medianStation1);
        q.setShort(8, medianTemperature1);
        
        if (medianRowId2 > -1) {
            q.setInt(9, medianRowId2);
            q.setInt(10, medianTime2);
            q.setInt(11, medianStation2);
            q.setShort(12, medianTemperature2);
        } else {
            q.setNull(9, java.sql.Types.INTEGER);
            q.setNull(10, java.sql.Types.INTEGER);
            q.setNull(11, java.sql.Types.INTEGER);
            q.setNull(12, java.sql.Types.SMALLINT);
        }
        
        q.setLong(13, rowCount);
        
        q.execute();
        
        log.info("Median info inserted");
    }
    
    @Override
    protected void calculateLeftShiftsForStation () throws SQLException {
        log.info("Calculating left shifts...");
        
        PreparedStatement q = conn.prepareStatement("SELECT s.station, s.order_id FROM " + sortedTable + " AS s " +
                                                    " ORDER BY s.station ASC");
                
        ResultSet res = q.executeQuery();
            
        calculateShiftsForStation(res, "left");
        
        res.close();
        q.close();
                
        log.info("Finished calculating left shifts");      
    }
    
    @Override
    protected void calculateRightShiftsForStation () throws SQLException {
        log.info("Calculating right shifts...");
        
        PreparedStatement q = conn.prepareStatement("SELECT s.station, s.order_id FROM " + sortedTable + " AS s " +
                                                    "  ORDER BY s.station DESC");
                
        ResultSet res = q.executeQuery();
            
        calculateShiftsForStation(res, "right");
        
        res.close();
        q.close();
        
        log.info("Finished calculating right shifts");      
    }
    
    protected void calculateShiftsForStation (ResultSet res, String dir) throws SQLException {
        int i = 0;
        int sumBelow = 0;
        int sumAbove = 0;
        int prevOrder = -1;
        
        String tempTableName = stationShiftTable + "_" + dir;
               
        Statement q = conn.createStatement();
        
        // drop any old version of table, if exists
        q.execute("DROP TABLE IF EXISTS " + tempTableName);
        
        // create new table
        q.execute("CREATE TEMP TABLE " + tempTableName + " ( " +
                     "  station int NOT NULL," +
                     "  shift int NOT NULL," +
                     "  CONSTRAINT " + tempTableName + "_pkey PRIMARY KEY (station)" +
                     ")"
                  );
        
        StringBuilder sb = new StringBuilder();
        CopyManager cpManager = ((PGConnection)conn).getCopyAPI();
        PushbackReader reader = new PushbackReader( new StringReader(""), 10000 );
        
        double compareOrder = medianOrder1;
        if (medianOrder2 != -1) {
            compareOrder = ((double)medianOrder1 + (double)medianOrder2) / 2;
        }     
        
        int prevStation = -1;
        while(res.next()) {
            int orderId = res.getInt("order_id");            
            int station = res.getInt("station");
                       
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

            if (prevStation != station) {
                prevStation = station;
                sb.append(station).append(",")
                    .append(shift).append("\n");
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
    protected void insertShiftsForStation() throws SQLException {
        log.info("Inserting station shift data...");
                               
        PreparedStatement insert = conn.prepareStatement(
                "INSERT INTO " + stationShiftTable + " (station, part, shift_left, shift_right) " +
                " SELECT l.station, " + this.partId + ", l.shift AS shift_left, r.shift AS shift_right" + 
                " FROM " + stationShiftTable + "_left AS l" + 
                " INNER JOIN " + stationShiftTable + "_right AS r ON l.station = r.station" +
                " ORDER BY l.station ASC"
        );
        
        insert.execute();      
        
        Statement q = conn.createStatement();
        
        // drop temporary tables
        q.execute("DROP TABLE " + stationShiftTable + "_left");
        q.execute("DROP TABLE " + stationShiftTable + "_right");
        
        log.info("Finished inserting station shift data");
    }

    @Override
    protected void calculateLeftShiftsForTime() throws SQLException {
        log.info("Calculating left shifts for time dimension...");
        
        PreparedStatement q = conn.prepareStatement("SELECT o.station, o.timed, s.order_id FROM " + config.getTable() + " AS o " +
                                                    " INNER JOIN " + sortedTable + " AS s " +
                                                    " ON o.id = s.row_id ORDER BY o.station ASC, o.timed ASC");
                
        ResultSet res = q.executeQuery();
            
        calculateShiftsForTime(res, "left");
        
        res.close();
        q.close();
                
        log.info("Finished calculating left shifts");
    }

    @Override
    protected void calculateRightShiftsForTime() throws SQLException {
        log.info("Calculating right shifts...");
        
        PreparedStatement q = conn.prepareStatement("SELECT o.station, o.timed, s.order_id FROM " + config.getTable() + " AS o " +
                                                    " INNER JOIN " + sortedTable + " AS s " +
                                                    " ON o.id = s.row_id ORDER BY o.station DESC, o.timed DESC");
                
        ResultSet res = q.executeQuery();
            
        calculateShiftsForTime(res, "right");
        
        res.close();
        q.close();
        
        log.info("Finished calculating right shifts"); 
    }
    
    protected void calculateShiftsForTime (ResultSet res, String dir) throws SQLException {
        int i = 0;
        int sumBelow = 0;
        int sumAbove = 0;
        int prevOrder = -1;
        
        String tempTableName = timeShiftTable + "_" + dir;
               
        Statement q = conn.createStatement();
        
        // drop any old version of table, if exists
        q.execute("DROP TABLE IF EXISTS " + tempTableName);
        
        // create new table
        q.execute("CREATE TEMP TABLE " + tempTableName + " ( " +
                     "  station int NOT NULL," +
                     "  timed int NOT NULL," +
                     "  shift int NOT NULL," +
                     "  CONSTRAINT " + tempTableName + "_pkey PRIMARY KEY (station, timed)" +
                     ")"
                  );
        
        StringBuilder sb = new StringBuilder();
        CopyManager cpManager = ((PGConnection)conn).getCopyAPI();
        PushbackReader reader = new PushbackReader( new StringReader(""), 10000 );
        
        double compareOrder = medianOrder1;
        if (medianOrder2 != -1) {
            compareOrder = ((double)medianOrder1 + (double)medianOrder2) / 2;
        }     
        
        int prevStation = -1;
        int prevTimed = -1;
        int prevShift = -1;
        while(res.next()) {
            int orderId = res.getInt("order_id");            
            int station = res.getInt("station");
            int timed = res.getInt("timed");
            
            // need to reset for new station?
            if (prevStation != station) {
                if (dir.equals("left")) {
                    sb.append(station).append(",")
                      .append("0").append(",")
                      .append("0").append("\n");     
                    
                    if (prevStation > -1) {
                        sb.append(prevStation).append(",")
                          .append(Integer.MAX_VALUE).append(",")
                          .append(prevShift).append("\n");
                    }
                } else {
                    sb.append(station).append(",")
                      .append(Integer.MAX_VALUE).append(",")
                      .append(0).append("\n");
                    
                    if (prevStation > -1) {
                        sb.append(prevStation).append(",")
                          .append(0).append(",")
                          .append(prevShift).append("\n");
                    }
                }
                
                prevStation = station;
                
                sumBelow = 0;
                sumAbove = 0;
                prevOrder = -1;
                prevTimed = -1;
                prevShift = -1;
            }
                       
            int shift = 0;
            prevShift = shift;
            
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

            if (prevTimed != timed) {
                prevTimed = timed;
                
                sb.append(station).append(",")
                  .append(timed).append(",")
                  .append(shift).append("\n");
                
                i++;
                            
                if (i % 200 == 0) {
                    try {
                        reader.unread( sb.toString().toCharArray() );
                        cpManager.copyIn("COPY " + tempTableName + " FROM STDIN WITH CSV", reader );
                        sb.delete(0,sb.length());
                    } catch (IOException e) {
                        throw new SQLException("Calculating time shifts failed", e);
                    }
                }

                if (i % 100000 == 0) {
                    log.info("Finished " + Startup.formatter.format(i) + " rows " + Math.floor(((double)i/(double)rowCount)*100) + "%");
                }
            }
            
            prevOrder = orderId;
        }  
        
        if (dir.equals("left")) {
            sb.append(prevStation).append(",")
              .append(Integer.MAX_VALUE).append(",")
              .append(prevShift).append("\n");
        } else {
            sb.append(prevStation).append(",")
              .append(0).append(",")
              .append(prevShift).append("\n");
        }
        
        try {
            reader.unread( sb.toString().toCharArray() );
            cpManager.copyIn("COPY " + tempTableName + " FROM STDIN WITH CSV", reader );
        } catch (IOException ex) {
            throw new SQLException("Calculating time shifts failed", ex);
        }       
    }

    @Override
    protected void insertShiftsForTime() throws SQLException {
        log.info("Inserting time shift data...");
                               
        PreparedStatement insert = conn.prepareStatement(
                "INSERT INTO " + timeShiftTable + " (timed, station, part, shift_left, shift_right) " +
                " SELECT l.timed, l.station, " + this.partId + ", l.shift AS shift_left, r.shift AS shift_right" + 
                " FROM " + timeShiftTable + "_left AS l" + 
                " INNER JOIN " + timeShiftTable + "_right AS r ON l.timed = r.timed AND l.station = r.station" +
                " ORDER BY l.station ASC, l.timed ASC"
        );
        
        insert.execute();      
        
        Statement q = conn.createStatement();
        
        // drop temporary tables
        q.execute("DROP TABLE " + timeShiftTable + "_left");
        q.execute("DROP TABLE " + timeShiftTable + "_right");
        
        log.info("Finished inserting time shift data");
    }
    
}
