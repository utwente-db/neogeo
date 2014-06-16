/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.fastfullwithcountmedian;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Logger;
import org.postgresql.pljava.ResultSetProvider;

/**
 *
 * @author Dennis
 */
public class KnmiMedian {
    public static ResultSetProvider knmi_median (String tableName, String columnName, int startTime, int endTime, int startStation, int endStation, int groupCount) throws SQLException {
        long startExecTime = System.nanoTime();  

        // create new Median Provider
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        MedianProvider provider = new MedianProvider(conn, tableName, columnName, startTime, endTime, startStation, endStation, groupCount);
        
        provider.prepare();
        
        // stop timer
        long execTime1 = System.nanoTime() - startExecTime;
        
        // do "off-the-clock" counting here
        // why? because we do not consider the counting to be part of our timed results
        provider.countRows();
        
        // start timer again
        startExecTime = System.nanoTime();
        
        // calculate medians
        provider.calculateMedians();
        
        // output final execution time
        long execTime2 = System.nanoTime() - startExecTime;
        long execTime = execTime1 + execTime2;
        log("Median runtime: " + execTime + " ns");     
        
        return provider;
    }
        
    public static ResultSetProvider knmi_median (String tableName, String columnName, int groupCount) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        
        PreparedStatement q = conn.prepareStatement("SELECT MIN(timed) AS start_time, MAX(timed) AS end_time, "
                + "MIN(station) AS start_station, MAX(station) AS end_station FROM " + tableName);
        
        ResultSet res = q.executeQuery();
        
        if (res.next() == false) {
            throw new SQLException("Unable to determine start/end time or start/end station of table");
        }
        
        int startTime = res.getInt("start_time");
        int endTime = res.getInt("end_time") + 1;
        int startStation = res.getInt("start_station");
        int endStation = res.getInt("end_station");
        
        res.close();
        q.close();
        
        return knmi_median (tableName, columnName, startTime, endTime, startStation, endStation, groupCount);
    }
        
    private static void log (String msg) {
        Logger.getAnonymousLogger().info(msg);
    }
    
    private static void logWarn (String msg) {
        Logger.getAnonymousLogger().warning(msg);
    }
    
    public static class Group {
        protected int startTime;
        
        protected int endTime;
        
        protected int startStation;
        
        protected int endStation;
        
        protected int rowCount;
        
        protected double median;
        
        protected int medianRowId1;
        
        protected int medianRowId2;
        
        protected int medianPos1 = -1;
        
        protected int medianPos2 = -1;
        
        protected int currPos = 0;
        
        protected MedianProvider provider;
        
        public Group (MedianProvider provider, int startTime, int endTime, int startStation, int endStation) {
            this.provider = provider;
            this.startTime = startTime;
            this.endTime = endTime;
            this.startStation = startStation;
            this.endStation = endStation;
        }
               
        public int getRowCount () {
            return this.rowCount;
        }
        
        public void setRowCount (int rowCount) {
            this.rowCount = rowCount;
            
            if (rowCount > 0) {
                if (rowCount % 2 == 0) {
                    medianPos1 = (rowCount/2) - 1;
                    medianPos2 = medianPos1 + 1;
                } else {
                    medianPos1 = (int) Math.floor(rowCount / 2);
                }
            }
        }
     
        public void setMedian (double median) {
            this.median = median;
        }
        
        public double getMedian () {
            return this.median;
        }
        
        public int[] getMedianRowIds () {
            int[] ret = {};
            
            if (medianPos1 > -1 && medianPos2 > -1) {
                ret = new int[]{medianRowId1, medianRowId2};
            } else if (medianPos1 > -1) {
                ret = new int[]{medianRowId1};
            }
            
            return ret;
        }

        /**
         * @return the startTime
         */
        public int getStartTime() {
            return startTime;
        }

        /**
         * @return the endTime
         */
        public int getEndTime() {
            return endTime;
        }

        /**
         * @return the startStation
         */
        public int getStartStation() {
            return startStation;
        }

        /**
         * @return the endStation
         */
        public int getEndStation() {
            return endStation;
        }
        
        public void addTuple (int rowId, int timed, int station) {            
            if (currPos == medianPos1) {
                 medianRowId1 = rowId;
                
                if (medianPos2 == -1) {
                    provider.notifyFoundMedian();
                }
            } else if (currPos == medianPos2) {
                medianRowId2 = rowId;
                provider.notifyFoundMedian();
            }
            
            currPos++;
        }
    }
    
    public static class MedianProvider implements ResultSetProvider {
        public static final int STATION_COUNT = 10;
        
        public static final int TIME_STEP = 600;
        
        protected String tableName;
        
        protected String columnName;
        
        protected int startTime;
        
        protected int endTime;
        
        protected int startStation;
        
        protected int endStation;
        
        protected int groupCount;
        
        protected Connection conn;
        
        protected Group[][] groupList;
        
        protected Group[] groupRefList;
        
        protected PreparedStatement fetchValueQuery;
        
        protected int level;
        
        protected int part;
        
        protected DataInputStream dataStream = null;
    
        protected int dataStreamSize = 0;

        protected int dataStreamPos = 0;
        
        protected int currDataId = 1;
        
        protected int mediansFoundCount = 0;
        
        protected double groupDivisor;
        
        protected int indexMinus;
        
        protected double shiftTime;
        
        protected ResultSet buffer;
        
        public MedianProvider(Connection conn, String tableName, String columnName, int startTime, int endTime, int startStation, int endStation, int groupCount) {
            this.conn = conn;
            this.tableName = tableName;
            this.columnName = columnName;
            this.startTime = startTime;
            this.endTime = endTime;
            this.startStation = startStation;
            this.endStation = endStation;
            this.groupCount = groupCount;  
        }
        
        public void notifyFoundMedian () {
            mediansFoundCount++;
        }
        
        public void prepare () throws SQLException {
            determineGroups();
            determineLevel();
        }
        
        public void countRows () throws SQLException {
            PreparedStatement countQuery = conn.prepareStatement("SELECT COUNT(*) AS row_count FROM " + tableName + " WHERE timed >= ? AND timed < ? "
                    + "AND station >= ? AND station <= ?");
            
            // determine number of rows for each group
            for(Group group : groupRefList) {
                countQuery.setInt(1, group.getStartTime());
                countQuery.setInt(2, group.getEndTime());
                countQuery.setInt(3, group.getStartStation());
                countQuery.setInt(4, group.getEndStation());
                
                ResultSet res = countQuery.executeQuery();
                res.next();
                
                group.setRowCount(res.getInt("row_count"));
                
                res.close();
            }
            
            countQuery.close();
        }
        
        public void calculateMedians () throws SQLException {
            scanData();
            determineMedians();
        }
                
        private void determineMedians () throws SQLException {
            fetchValueQuery = conn.prepareStatement("SELECT temperature FROM " + tableName + " WHERE id = ? LIMIT 1");
            
            for(Group group : groupRefList) {
                int[] medianRowIds = group.getMedianRowIds();
                double median = 0;
                
                if (medianRowIds.length == 2) {
                    median = (fetchValueForRowId(medianRowIds[0]) + fetchValueForRowId(medianRowIds[1])) / 2;
                } else if (medianRowIds.length == 1) {
                    median = fetchValueForRowId(medianRowIds[0]);
                }
                
                group.setMedian(median);
            }
            
            fetchValueQuery.close();
        }
        
        
        private boolean scanData () throws SQLException {
            Statement q = conn.createStatement();
            buffer = q.executeQuery("SELECT size, data FROM " + tableName + "_level" + level + "_data_full WHERE part = " + this.part + " ORDER BY id ASC");
            
            while(true) {
                if (dataStream == null || dataStreamSize == dataStreamPos) {
                    if (getNextBuffer() == false) {
                        break;
                    }
                }
                
                try {
                    int rowId = dataStream.readInt();
                    int timed = dataStream.readInt();
                    int station = dataStream.readByte();
                    handleTuple(rowId, timed, station);
                } catch (EOFException e) {
                    if (getNextBuffer() == false) {
                        break;
                    }
                } catch (IOException e) {
                    throw new SQLException("IOException whilst trying to read next time");
                }
                
                if (groupCount == mediansFoundCount) {
                    log("Found all medians!");
                    break;
                }
            }
            
            buffer.close();
            q.close();
            
            return true;
        }
        
        protected boolean getNextBuffer () throws SQLException {
            if (buffer.next() == false) {
                return false;
            }

            byte[] asBytes = buffer.getBytes("data");
            ByteArrayInputStream bin = new ByteArrayInputStream(asBytes);
            dataStream = new DataInputStream(bin);
            dataStreamSize = buffer.getInt("size");
            dataStreamPos = 0;

            return true;
        }
        
        protected void handleTuple (int rowId, int timed, int station) {
            // ignore if it falls outside range
            if (timed < startTime || timed >= endTime || station < startStation || station > endStation) {
                return;
            }
            
            // only a single group? then it's easy, just add to that group
            if (groupCount == 1) {
                this.groupList[0][0].addTuple(rowId, timed, station);
            } else if (groupCount == 10) {
                this.groupList[station-1][0].addTuple(rowId, timed, station);
            } else {
                int index = (int) Math.floor(((double)timed - shiftTime) / groupDivisor ) - indexMinus;
                
                if (index >= 0 && index < groupList.length) {
                    this.groupList[station-1][index].addTuple(rowId, timed, station);
                }
            }
        }
        
        private void determineLevel () throws SQLException {
            // find the level/part the current range fits into
            PreparedStatement q = conn.prepareStatement("SELECT level, part FROM " + tableName + "_full_index WHERE start_time <= ? AND end_time >= ? AND enabled = 1"
                    + "ORDER BY row_count ASC LIMIT 1");
            q.setLong(1, startTime);
            q.setLong(2, endTime);
            
            ResultSet res = q.executeQuery();
            if (res.next() == false) {
                res.close();
                q.close();
                
                q = conn.prepareStatement("SELECT * FROM " + tableName + "_full_index WHERE level = 1 AND part = 1");
                res = q.executeQuery();

                if (res.next() == false) {
                    res.close();
                    q.close();
                    throw new SQLException("Can't find any pre-calculated median info in median table!");
                }      
            }
            
            level = res.getInt("level");
            part = res.getInt("part");            
            
            res.close();
            q.close();
            
            log("Using part #" + part + " of level #" + level);
        }
        
        private void determineGroups () {
            groupRefList = new Group[groupCount];
            
            if (groupCount == 1) {
                groupList = new Group[1][1];
                groupList[0][0] = new Group(this, startTime, endTime, startStation, endStation);
                groupRefList[0] = groupList[0][0];
            } else if (groupCount == STATION_COUNT) {
                groupList = new Group[STATION_COUNT][1];

                for(int i=startStation; i <= endStation; i++) {
                    groupList[i-1][0] = new Group(this, startTime, endTime, i, i);
                    groupRefList[i-1] = groupList[i-1][0];
                }               
            } else if (groupCount > STATION_COUNT) {
                int range = endTime - startTime;
                int groupPerStationCount = groupCount/STATION_COUNT;
                this.groupDivisor = range / groupPerStationCount;
                this.indexMinus = (int) Math.floor((double)startTime / groupDivisor );
                
                // This value is used to shift the timed values back so that the starttime gets value of 0.0000~ instead of e.g. 0.424
                shiftTime = (((double)startTime / groupDivisor ) - (double)indexMinus) * (double)groupDivisor;
                
                groupList = new Group[STATION_COUNT][groupPerStationCount];
                
                for(int i=startStation-1; i < endStation; i++) {
                    int minTime = startTime;
                    for(int j=0; j < groupPerStationCount; j++) {
                        int maxTime = minTime + (int)groupDivisor;
                        
                        groupList[i][j] = new Group(this, minTime, maxTime, i+1, i+1);
                        groupRefList[(i*STATION_COUNT) + j] = groupList[i][j];
                        
                        minTime = maxTime;
                    }
                }
                
            } else {
                throw new UnsupportedOperationException("Groupcount smaller than 10 is not yet supported");
            }
        }
        
        protected double fetchValueForRowId(int rowId) throws SQLException {            
            fetchValueQuery.setInt(1, rowId);
            ResultSet res = fetchValueQuery.executeQuery();
           
            if (res.next() == false) {
                log("Unable to fetch value for rowId " + rowId);
                return -1;
            }
           
            double value = res.getDouble("temperature");           
            res.close();           
           
            return value;
        }

        public boolean assignRowValues(ResultSet receiver, int currentRow) throws SQLException {
            if (currentRow >= groupRefList.length) {
                return false;
            }
            
            Group currGroup = groupRefList[currentRow];
                        
            receiver.updateInt(1, currentRow);
            receiver.updateInt(2, currGroup.getRowCount());
            receiver.updateLong(3, currGroup.getStartTime());
            receiver.updateLong(4, currGroup.getEndTime());
            receiver.updateLong(5, currGroup.getStartStation());
            receiver.updateLong(6, currGroup.getEndStation());
            receiver.updateDouble(7, currGroup.getMedian());
            
            return true;
        }

        public void close() throws SQLException {
            // not needed yet
        }
        
        
    }
    
}
