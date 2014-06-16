/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.fastmedian;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Logger;
import org.postgresql.pljava.ResultSetProvider;

/**
 *
 * @author Dennis
 */
public class FastKnmiMedian {
    
    public static ResultSetProvider knmi_median (String tableName, String columnName, int startTime, int endTime, int startStation, int endStation, int groupCount) throws SQLException {
        long startExecTime = System.nanoTime();  

        // create new Median Provider
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        MedianProvider provider = new MedianProvider(conn, tableName, columnName, startTime, endTime, startStation, endStation, groupCount);
               
        // calculate medians
        provider.calculateMedians();
        
        // output final execution time
        long execTime = System.nanoTime() - startExecTime;
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
        protected enum SHIFT_DIRECTION {
            Left, Right
        };
        
        public final static int STATION_STEP = 1;
        
        protected int startTime;
        
        protected int endTime;
        
        protected int startStation;
        
        protected int endStation;
        
        protected int rowCount;
        
        protected double median = 0;
        
        protected int level;
    
        protected int part;

        protected int medianRowId1 = -1;
        
        protected int medianTime1 = -1;
        
        protected int medianStation1 = -1;

        protected int medianRowId2= -1;
        
        protected int medianTime2 = -1;
        
        protected int medianStation2 = -1;
        
        protected boolean isEven = false;
        
        protected MedianProvider provider;
        
        protected Connection conn;
        
        protected String tableName;
        
        protected int sumShift = 0;
        
        protected int newMedianRowId1 = -1;
        
        protected int newMedianRowId2 = -1;
        
        protected DataInputStream rightStream = null;
    
        protected int rightStreamSize = 0;

        protected int rightStreamPos = 0;

        protected DataInputStream leftStream = null;

        protected int leftStreamSize = 0;

        protected int leftStreamPos = 0;
        
        protected int currLeftId = -1;
    
        protected int currRightId = -1;
        
        public Group (MedianProvider provider, int startTime, int endTime, int startStation, int endStation) {
            this.provider = provider;
            this.startTime = startTime;
            this.endTime = endTime;
            this.startStation = startStation;
            this.endStation = endStation;
            this.conn = provider.getConnection();
            this.tableName = provider.getTableName();
        }
               
        public int getRowCount () {
            return -1;
        }
             
        public void setMedian (double median) {
            this.median = median;
        }
        
        public double getMedian () {
            return this.median;
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
        
        protected void calculateShift () throws SQLException {
            // calculate left shift for station dimension
            sumShift += this.getShiftForStation(startStation, "left");
            
            // calculate right shift for station dimension
            sumShift += this.getShiftForStation(endStation, "right");
            
            // calculate shift for time dimension
            for(int station=startStation; station <= endStation; station += STATION_STEP) {
                sumShift += this.getShiftForTime(station, startTime, "left");
                sumShift += this.getShiftForTime(station, endTime, "right");
            }
        }
        
        public void calculateMedian () throws SQLException {
             // load median information
            try {
                this.loadMedianInfo();
            } catch (SQLException e) {
                throw new SQLException("Unable to load pre-calculated median info for table " + tableName);
            }
            
            // calculate shift for this group
            this.calculateShift();
            log("Sum shift: " + sumShift);
            
            if (isEven) {
                this.calculateMedianEven(sumShift);
            } else {
                this.calculateMedianOdd(sumShift);
            }
            
            // have we been able to determine the new median?
            if (newMedianRowId1 == -1) {
                return;
            }

            // determine new median
            double value1 = provider.fetchValueForRowId(newMedianRowId1);
            median = value1;
            if (newMedianRowId2 > 0) {
                double value2 = provider.fetchValueForRowId(newMedianRowId2);
                median = ((value1 + value2) / 2);
            }
        }
        
        protected void calculateMedianOdd (int sumShift) throws SQLException {
            // is dataset median part of range?
            boolean isMedianIncluded = (medianTime1 >= startTime && medianTime1 < endTime && medianStation1 >= startStation && medianStation1 <= endStation);

            if (sumShift == 0) {
                this.calculateMedianOddNoShift(isMedianIncluded);
            } else {
                this.calculateMedianOddWithShift(sumShift, isMedianIncluded);
            }
        }
        
        protected void calculateMedianEven (int sumShift) throws SQLException {
            boolean isLeftMedianIncluded = (medianTime1 >= startTime && medianTime1 < endTime && medianStation1 >= startStation && medianStation1 <= endStation);
            boolean isRightMedianIncluded = (medianTime2 >= startTime && medianTime2 < endTime && medianStation2 >= startStation && medianStation2 <= endStation);

            if (sumShift == 0) {
                this.calculateMedianEvenNoShift(isLeftMedianIncluded, isRightMedianIncluded);
            } else {
                this.calculcateMedianEvenWithShift(sumShift, isLeftMedianIncluded, isRightMedianIncluded);
            }
        }
        
        protected void calculateMedianOddWithShift(int sumShift, boolean isMedianIncluded) throws SQLException {
            // compensate for missing median?
            if (isMedianIncluded == false) {
                if (sumShift < 0) {
                    sumShift--;
                } else {
                    sumShift++;
                }
            }

            // set current median set
            int[] currSet = {medianRowId1};

            // calculate new median
            calculateMedianWithShift (sumShift, currSet);
        }

        protected void calculateMedianOddNoShift(boolean isMedianIncluded) throws SQLException {
            if (isMedianIncluded) {
                this.newMedianRowId1 = medianRowId1;
                return;
            } else {
                // split median into its nearest neighbours
                // find new left neighbour for median
                this.newMedianRowId1 = getNextLeftRowId();

                // find new left neihbour for median
                this.newMedianRowId2 = getNextRightRowId();
            }
        }    

        protected void calculcateMedianEvenWithShift(int sumShift, boolean isLeftMedianIncluded, boolean isRightMedianIncluded) throws SQLException {
            // compensate for excluding medians
            if (isLeftMedianIncluded == false && sumShift < 0) {
                sumShift += -2;
            } else if (isRightMedianIncluded == false && sumShift > 0) {
                sumShift += 2;
            }

            // set current median set
            int[] currSet = {medianRowId1, medianRowId2};

            // calculate new median
            calculateMedianWithShift (sumShift, currSet);
        }

        protected void calculateMedianEvenNoShift(boolean isLeftMedianIncluded, boolean isRightMedianIncluded) throws SQLException {
            // if both left and right median are included
            // and no shift then new median is exactly the current median
            if (isLeftMedianIncluded && isRightMedianIncluded) {
                this.newMedianRowId1 = medianRowId1;
                this.newMedianRowId2 = medianRowId2;
                return;
            }

            if (isLeftMedianIncluded == false) {
                // find new left neighbour for median
                this.newMedianRowId1 = getNextLeftRowId();
            }

            if (isRightMedianIncluded == false) {
                // find new left neihbour for median
                this.newMedianRowId2 = getNextRightRowId();
            }        
        }

        protected void calculateMedianWithShift (int sumShift, int[] currSet) throws SQLException {        
            if (sumShift < 0) {
                while (sumShift < 0) {
                    // current median set consists of two parts?
                    // then move 1 to the left by taking left part of current median
                    if (currSet.length == 2) {
                        currSet = new int[]{currSet[0]};
                        sumShift++;                    
                    } else {
                        // find next left neighbour
                        int nextRowId = getNextLeftRowId();
                        currSet = new int[]{nextRowId, currSet[0]};
                        sumShift++;
                    }  
                }
            } else {
                while(sumShift > 0) {
                    // current median set consists of two parts?
                    // then move 1 to the right by taking right part of current median
                    if (currSet.length == 2) {
                        currSet = new int[]{currSet[1]};
                        sumShift--;                    
                    } else {
                        // find next right neighbour
                        int nextRowId = getNextRightRowId();
                        currSet = new int[]{currSet[0], nextRowId};
                        sumShift--;
                    }              
                }            
            }

            if (currSet.length == 2) {
                newMedianRowId1 = currSet[0];
                newMedianRowId2 = currSet[1];
            } else {
                newMedianRowId1 = currSet[0];
            }
        }

        protected int getNextRightRowId () throws SQLException{
            if (rightStream == null || rightStreamSize == rightStreamPos) {
                if (getNextRightBuffer() == false) {
                    return 0;
                }
            }

            int nextRowId = 0;

            while(nextRowId == 0) {
                int currRowId = -1;
                int currTime = -1;
                int currStation = -1;
                try {
                    currRowId = rightStream.readInt();
                    currTime = rightStream.readInt();
                    currStation = rightStream.readInt();
                } catch (IOException ex) {
                    throw new SQLException("IOException whilst trying to get next right neighbour");
                }
                rightStreamPos++;

                if (currTime >= this.startTime && currTime < this.endTime && currStation >= this.startStation && currStation <= this.endStation) {
                    nextRowId = currRowId;
                    break;
                }        

                if (rightStreamSize == rightStreamPos) {
                    if (getNextRightBuffer() == false) {
                        break;
                    }
                }
            }

            return nextRowId;
        }

        protected boolean getNextRightBuffer () throws SQLException {
            // fetch next buffer for left tuples
            PreparedStatement q = conn.prepareStatement("SELECT size, data FROM " + tableName + "_level" + level + "_data WHERE direction = 1 AND part = ? AND id > ?");
            q.setInt(1, this.part);
            q.setInt(2, currRightId);

            // increment current right ID to ensure that next buffer fetch retrieves next buffer
            currRightId++;

            ResultSet res = q.executeQuery();
            if (res.next() == false) {
                logWarn("Unable to fetch next right buffer");

                res.close();
                q.close();
                return false;
            }

            byte[] asBytes = res.getBytes("data");
            ByteArrayInputStream bin = new ByteArrayInputStream(asBytes);
            rightStream = new DataInputStream(bin);
            rightStreamSize = res.getInt("size");
            rightStreamPos = 0;

            res.close();
            q.close();
            return true;
        }

        protected int getNextLeftRowId () throws SQLException{
            if (leftStream == null || leftStreamSize == leftStreamPos) {
                if (getNextLeftBuffer() == false) {
                    return 0;
                }
            }

            int nextRowId = 0;

            while(nextRowId == 0) {
                int currRowId = -1;
                int currTime = -1;
                int currStation = -1;
                
                try {
                    currRowId = leftStream.readInt();
                    currTime = leftStream.readInt();
                    currStation = leftStream.readInt();
                } catch (IOException ex) {
                    throw new SQLException("IOException whilst trying to get next left neighbour");
                }
                leftStreamPos++;

                if (currTime >= this.startTime && currTime < this.endTime && currStation >= this.startStation && currStation <= this.endStation) {
                    nextRowId = currRowId;
                    break;
                }        

                if (leftStreamSize == leftStreamPos) {
                    if (getNextLeftBuffer() == false) {
                        break;
                    }
                }
            }

            return nextRowId;
        }

        protected boolean getNextLeftBuffer () throws SQLException {
            // fetch next buffer for left tuples
            PreparedStatement q = conn.prepareStatement("SELECT size, data FROM " + tableName + "_level" + level + "_data WHERE direction = -1 AND part = ? AND id > ?");
            q.setInt(1, this.part);
            q.setInt(2, currLeftId);

            // increment current left ID to ensure that next buffer fetch retrieves next buffer
            currLeftId++;

            ResultSet res = q.executeQuery();
            if (res.next() == false) {
                logWarn("Unable to fetch next left buffer");

                res.close();
                q.close();
                return false;
            }

            byte[] asBytes = res.getBytes("data");
            ByteArrayInputStream bin = new ByteArrayInputStream(asBytes);
            leftStream = new DataInputStream(bin);
            leftStreamSize = res.getInt("size");
            leftStreamPos = 0;

            res.close();
            q.close();
            return true;
        }
        
        protected void loadMedianInfo () throws SQLException {
            PreparedStatement q = conn.prepareStatement("SELECT * FROM " + tableName + "_median " +
                                                        " WHERE enabled != 0 AND start_time <= ? AND end_time >= ? ORDER BY row_count ASC LIMIT 1");

            q.setLong(1, startTime);
            q.setLong(2, endTime);

            ResultSet res = q.executeQuery();
            if (res.next() == false) {
                res.close();
                q.close();

                q = conn.prepareStatement("SELECT * FROM " + tableName + "_median WHERE level = 1 AND part = 1");

                res = q.executeQuery();

                if (res.next() == false) {
                    res.close();
                    q.close();
                    throw new SQLException("Can't find any pre-calculated median info in median table!");
                }            
            }

            level = res.getInt("level");
            part = res.getInt("part");
            medianRowId1 = res.getInt("median_rowid_1");
            medianTime1 = res.getInt("median_time_1");
            medianStation1 = res.getInt("median_station_1");
            medianRowId2 = res.getInt("median_rowid_2");
            medianTime2 = res.getInt("median_time_2");
            medianStation2 = res.getInt("median_station_2");

            log("Using part #" + part + " of level " + level);

            isEven = (medianRowId2 > 0);

            res.close();
            q.close();
        }
        
        protected int getShiftForTime (int station, int time, String shiftType) throws SQLException {
            String sql = "SELECT shift_" + shiftType + " AS shift FROM " + tableName + "_level" + level + "_time_shift WHERE part = " + this.part + " AND "
                    + " station = " + station + " AND ";
            if (shiftType.equalsIgnoreCase("left")) {
                sql += "timed >= " + time + " ORDER BY timed ASC";
            } else {
                sql += "timed <= " + time + " ORDER BY timed DESC";
            }
            sql += " LIMIT 1";
            
            PreparedStatement q = conn.prepareStatement(sql);        
            ResultSet res = q.executeQuery();

            if (res.next() == false) {
                throw new SQLException("Unable to retrieve " + shiftType + " shift for tuple with station " + station + " and time " + time);
            }

            int shift = res.getInt("shift");

            res.close();
            q.close();

            return shift;
        }
        
        protected int getShiftForStation(int station, String shiftType) throws SQLException {
            String sql = "SELECT shift_" + shiftType + " AS shift FROM " + tableName + "_level" + level + "_station_shift WHERE part = " + this.part + " AND ";
            if (shiftType.equalsIgnoreCase("left")) {
                sql += "station >= " + station + " ORDER BY station ASC";
            } else {
                sql += "station <= " + station + " ORDER BY station DESC";
            }
            sql += " LIMIT 1";

            PreparedStatement q = conn.prepareStatement(sql);        
            ResultSet res = q.executeQuery();

            if (res.next() == false) {
                throw new SQLException("Unable to retrieve " + shiftType + " shift for tuple with station " + station);
            }

            int shift = res.getInt("shift");

            res.close();
            q.close();

            return shift;
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
     
        protected double groupDivisor;
        
        protected int indexMinus;
        
        protected double shiftTime;
        
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
        
        public Connection getConnection () {
            return this.conn;
        }
        
        public String getTableName () {
            return this.tableName;
        }
        
        public void calculateMedians () throws SQLException {
            fetchValueQuery = conn.prepareStatement("SELECT temperature FROM " + tableName + " WHERE id = ? LIMIT 1");
            
            determineGroups();
            determineMedians();
        }
                
        private void determineMedians () throws SQLException {
            for(Group group : groupRefList) {
                group.calculateMedian();
            }
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
                        int maxTime = minTime + (int)groupDivisor - TIME_STEP;
                        
                        groupList[i][j] = new Group(this, minTime, maxTime, i+1, i+1);
                        groupRefList[(i*STATION_COUNT) + j] = groupList[i][j];
                        
                        minTime = maxTime;
                    }
                }
                
            } else {
                throw new UnsupportedOperationException("Groupcount smaller than 10 is not yet supported");
            }
        }
        
        public double fetchValueForRowId(int rowId) throws SQLException {            
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
            fetchValueQuery.close();
        }
        
        
    }
}
