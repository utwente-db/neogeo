/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.fastshiftwithvaluesmedian;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;
import org.postgresql.pljava.ResultSetProvider;

/**
 *
 * @author Dennis
 */
public class KnmiMedian {
    public static final boolean DEBUG = false;
    
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
        public final static int STATION_STEP = 1;
        
        protected int startTime;
        
        protected int endTime;
        
        protected int startStation;
        
        protected int endStation;
               
        protected double median = -1;
        
        protected MedianProvider provider;
        
        protected int sumShift = 0;
        
        protected boolean sumShiftSet = false;
        
        protected boolean isLeftMedianIncluded = false;
        
        protected boolean isRightMedianIncluded = false;
        
        protected int newMedian1 = 0;
        
        protected int newMedian2 = 0;
                
        protected boolean hasNewMedian2 = false;       
                
        protected short newMedianValue1 = Short.MIN_VALUE;
        
        protected short newMedianValue2 = Short.MIN_VALUE;
        
        protected int leftPos = 0;
        
        protected int rightPos = 0;
        
        public Group (MedianProvider provider, int startTime, int endTime, int startStation, int endStation) {
            this.provider = provider;
            this.startTime = startTime;
            this.endTime = endTime;
            this.startStation = startStation;
            this.endStation = endStation;
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
        
        public boolean isSumShiftSet() {
            return sumShiftSet;
        }
        
        protected void calculateShift (PreparedStatement leftStationShiftQuery, PreparedStatement rightStationShiftQuery,
                PreparedStatement leftTimeShiftQuery, PreparedStatement rightTimeShiftQuery) throws SQLException {
            try {
                // calculate left shift for station dimension
                sumShift += this.getShiftForStation(leftStationShiftQuery, startStation);

                // calculate right shift for station dimension
                sumShift += this.getShiftForStation(rightStationShiftQuery, endStation);

                // calculate shift for time dimension
                for(int station=startStation; station <= endStation; station += STATION_STEP) {
                    sumShift += this.getShiftForTime(leftTimeShiftQuery, station, startTime);
                    sumShift += this.getShiftForTime(rightTimeShiftQuery, station, endTime);
                }

                this.setSumShift(sumShift);
            } catch (SQLException e) {
                logWarn("Unable to determine sumShift for group: " + e.getMessage());
            }
            
            isLeftMedianIncluded = (provider.getMedianTime1() >= startTime && provider.getMedianTime1() < endTime 
                                    && provider.getMedianStation1() >= startStation && provider.getMedianStation1() <= endStation);
            isRightMedianIncluded = (provider.isEven()
                                     && provider.getMedianTime2() >= startTime && provider.getMedianTime2() < endTime 
                                     && provider.getMedianStation2() >= startStation && provider.getMedianStation2() <= endStation);
        }
        
        protected int getShiftForTime (PreparedStatement query, int station, int time) throws SQLException {
            query.setInt(1, station);
            query.setInt(2, time);
            ResultSet res = query.executeQuery();

            if (res.next() == false) {
                throw new SQLException("Unable to retrieve time shift for tuple with station " + station + " and time " + time);
            }

            int shift = res.getInt("shift");

            res.close();

            return shift;
        }
        
        protected int getShiftForStation(PreparedStatement query, int station) throws SQLException {
            query.setInt(1, station);
            ResultSet res = query.executeQuery();

            if (res.next() == false) {
                throw new SQLException("Unable to retrieve shift for tuple with station " + station);
            }

            int shift = res.getInt("shift");

            res.close();

            return shift;
        }
        
        protected int getSumShift () {
            return this.sumShift;
        }
        
        protected void setSumShift (int sumShift) {
            this.sumShift = sumShift;
            this.sumShiftSet = true;
        } 
        
        public boolean isLeftMedianIncluded() {
            return isLeftMedianIncluded;
        }

      
        public boolean isRightMedianIncluded() {
            return isRightMedianIncluded;
        }

        public int getNewMedian1() {
            return newMedian1;
        }

        public void setNewMedian1(int newMedian1) {
            this.newMedian1 = newMedian1;
        }

        public int getNewMedian2() {
            return newMedian2;
        }

        public void setNewMedian2(int newMedian2) {
            this.newMedian2 = newMedian2;
            this.hasNewMedian2 = true;
        }
        
        public void setHasNewMedian2 (boolean val) {
            this.hasNewMedian2 = val;
            this.newMedianValue2 = Short.MIN_VALUE;
        }
        
        public short getNewMedianValue1 () {
            return newMedianValue1;
        }

        public void setNewMedianValue1(short value) {
            this.newMedianValue1 = value;
        }
        
        public short getNewMedianValue2 () {
            return newMedianValue2;
        }

        public void setNewMedianValue2(short value) {
            this.newMedianValue2 = value;
        }
        
        public void addTuple(short value, int dir) {
            if (dir < 0) {
                addTupleLeft(value);
            } else {
                addTupleRight(value);
            }            
        }
        
        protected void addTupleLeft(short value) {
            leftPos--;
            
            if (newMedian1 == leftPos) {
                newMedianValue1 = value;
                provider.notifyLeft();
            } else if (hasNewMedian2 && newMedian2 == leftPos) {
                newMedianValue2 = value;
                provider.notifyLeft();
            }
        }
        
        protected void addTupleRight(short value) {
            rightPos++;
            
            if (newMedian1 == rightPos) {
                newMedianValue1 = value;
                provider.notifyRight();
            } else if (hasNewMedian2 && newMedian2 == rightPos) {
                newMedianValue2 = value;
                provider.notifyRight();
            }
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
     
        protected double groupDivisor;
        
        protected int indexMinus;
        
        protected double shiftTime;
                        
        protected int scanLeft = 0;
        
        protected int scanRight = 0;
        
        protected DataInputStream leftStream = null;
    
        protected int leftStreamSize = 0;

        protected int leftStreamPos = 0;
        
        protected DataInputStream rightStream = null;
    
        protected int rightStreamSize = 0;

        protected int rightStreamPos = 0;
        
        protected int scanRightTotal = 0;
        
        protected int scanLeftTotal = 0;
                
        protected int level;
    
        protected int part;
       
        protected int medianTime1 = -1;
        
        protected int medianStation1 = -1;
        
        protected short medianValue1 = -1;
        
        protected int medianTime2 = -1;
        
        protected int medianStation2 = -1;
        
        protected short medianValue2 = -1;
        
        protected boolean isEven = false;
        
        protected long execStartTime = -1;
        
        protected long execTime = -1;
        
        protected ResultSet leftBuffer;
        
        protected ResultSet rightBuffer;
                
        public MedianProvider(Connection conn, String tableName, String columnName, int startTime, int endTime, int startStation, int endStation, int groupCount) {
            this.conn = conn;
            this.tableName = tableName;
            this.columnName = columnName;
            this.startTime = startTime;
            this.endTime = endTime;
            
            // fix for overscan
            this.endTime++;
            
            this.startStation = startStation;
            this.endStation = endStation;
            this.groupCount = groupCount;  
        }
        
        public void notifyLeft () {
            scanLeft--;
        }
        
        public void notifyRight () {
            scanRight--;
        }
        
        public Connection getConnection () {
            return this.conn;
        }
        
        public String getTableName () {
            return this.tableName;
        }
        
        public void calculateMedians () throws SQLException {            
            loadMedianInfo();
            determineGroups();
            
            if (DEBUG) {
                execStartTime = System.nanoTime();
            }
            
            calculateShifts();
            
            if (DEBUG) {
                execTime = System.nanoTime() - execStartTime;
                log("Calculating shifts: " + (execTime/1000000) + " ms");
            }
            
            if (DEBUG) {
                execStartTime = System.nanoTime();
            }
            
            scanData();
            
            if (DEBUG) {
                execTime = System.nanoTime() - execStartTime;
                log("Scantime: " + (execTime/1000000) + " ms");
                log("Total left scan: " + scanLeftTotal);
                log("Total right scan: " + scanRightTotal);
            }
            
            if (DEBUG) {
                execStartTime = System.nanoTime();
            } 
            
            calculateMedianValues();
            
            if (DEBUG) {
                execTime = System.nanoTime() - execStartTime;
                log("Determining median's: " + (execTime/1000000) + " ms");
            }
        }
        
        protected void calculateMedianValues () throws SQLException {           
            for (Group group : groupRefList) {
                if (group.isSumShiftSet() == false) continue;
                
                short value1 = group.getNewMedianValue1();
                short value2 = group.getNewMedianValue2();
                
                if (value1 == Short.MIN_VALUE) continue;
                                
                double median = (double) value1;
                if (value2 > Short.MIN_VALUE) {
                    median = ((double)(value1 + value2)) / 2;
                }
                
                group.setMedian(median);
            }
        }
        
        protected void scanData() throws SQLException {            
            scanLeft();
            scanRight();
        }
        
        protected void scanRight () throws SQLException {            
            Statement q = conn.createStatement();
            rightBuffer = q.executeQuery("SELECT id, size, data FROM " + tableName + "_level" + level + "_data_withvalues WHERE direction = 1 AND part = 1 ORDER BY id ASC");
            
            while(scanRight > 0) {
                if (rightStream == null || rightStreamSize == rightStreamPos) {
                    if (getNextRightBuffer() == false) {
                        break;
                    }
                }

                int currTime = -1;
                int currStation = -1;
                short currValue = Short.MIN_VALUE;
                try {
                    currTime = rightStream.readInt();
                    currStation = rightStream.readByte();
                    currValue = rightStream.readShort();
                } catch (IOException ex) {
                    throw new SQLException("IOException whilst trying to get next right neighbour");
                }
                rightStreamPos++;
                
                scanRightTotal++;
                
                handleTuple(currStation, currTime, currValue, 1);
            }
            
            rightBuffer.close();
            q.close();
        }
        
        protected boolean getNextRightBuffer () throws SQLException {            
            if (rightBuffer.next() == false) {
                logWarn("Unable to fetch next right buffer");
                return false;
            }
            
            byte[] asBytes = rightBuffer.getBytes("data");
            ByteArrayInputStream bin = new ByteArrayInputStream(asBytes);
            rightStream = new DataInputStream(bin);
            rightStreamSize = rightBuffer.getInt("size");
            rightStreamPos = 0;

            return true;
        }
        
        protected void scanLeft () throws SQLException {              
            Statement q = conn.createStatement();
            leftBuffer = q.executeQuery("SELECT id, size, data FROM " + tableName + "_level" + level + "_data_withvalues WHERE direction = -1 AND part = 1 ORDER BY id ASC");
                        
            while(scanLeft > 0) {
                if (leftStream == null || leftStreamSize == leftStreamPos) {
                    if (getNextLeftBuffer() == false) {
                        break;
                    }
                }
                
                int currTime = -1;
                int currStation = -1;
                short currValue = Short.MIN_VALUE;
                try {
                    currTime = leftStream.readInt();
                    currStation = leftStream.readByte();
                    currValue = leftStream.readShort();
                } catch (IOException ex) {
                    throw new SQLException("IOException whilst trying to get next left neighbour");
                }
                leftStreamPos++;
                
                scanLeftTotal++;
                
                handleTuple(currStation, currTime, currValue, -1);
            }
            
            leftBuffer.close();
            q.close();
        }
        
        protected boolean getNextLeftBuffer () throws SQLException {
            if (leftBuffer.next() == false) {
                logWarn("Unable to fetch next left buffer");
                return false;
            }
            
            byte[] asBytes = leftBuffer.getBytes("data");
            ByteArrayInputStream bin = new ByteArrayInputStream(asBytes);
            leftStream = new DataInputStream(bin);
            leftStreamSize = leftBuffer.getInt("size");
            leftStreamPos = 0;

            return true;
        }
        
        private void handleTuple (int station, int timed, short value, int dir) {
            // ignore if it falls outside range
            if (timed < startTime || timed >= endTime || station < startStation || station > endStation) {
                return;
            }
                       
            // only a single group? then it's easy, just add to that group
            if (groupCount == 1) {
                this.groupList[0][0].addTuple(value, dir);
            } else if (groupCount == 10) {
                this.groupList[station-1][0].addTuple(value, dir);
            } else {
                int index = (int) Math.floor(((double)timed - shiftTime) / groupDivisor ) - indexMinus;
                
                if (index >= 0 && index < groupList.length) {
                    this.groupList[station-1][index].addTuple(value, dir);
                }
            }
        }
        
        protected void calculateShifts () throws SQLException {              
            PreparedStatement leftStationShiftQuery = conn.prepareStatement(
                                                      "SELECT shift_left AS shift FROM " + tableName + "_level" + level + "_station_shift WHERE part = " + this.part +
                                                      " AND station >= ? ORDER BY station ASC LIMIT 1"
                                                      );
            PreparedStatement rightStationShiftQuery = conn.prepareStatement(
                                                      "SELECT shift_right AS shift FROM " + tableName + "_level" + level + "_station_shift WHERE part = " + this.part +
                                                      " AND station <= ? ORDER BY station DESC LIMIT 1"
                                                      );
            
            PreparedStatement leftTimeShiftQuery = conn.prepareStatement(
                                                   "SELECT shift_left AS shift FROM " + tableName + "_level" + level + "_time_shift WHERE part = " + this.part +
                                                   " AND station = ? AND timed >= ? ORDER BY timed ASC LIMIT 1"
                                                   );
            
            PreparedStatement rightTimeShiftQuery = conn.prepareStatement(
                                                   "SELECT shift_right AS shift FROM " + tableName + "_level" + level + "_time_shift WHERE part = " + this.part +
                                                   " AND station = ? AND timed <= ? ORDER BY timed DESC LIMIT 1"
                                                   );
            
            for(Group group : groupRefList) {
                group.calculateShift(leftStationShiftQuery, rightStationShiftQuery, leftTimeShiftQuery, rightTimeShiftQuery);
                this.compensateShift(group);
                calculateNewMedian(group);
            }
            
            leftStationShiftQuery.close();
            rightStationShiftQuery.close();
            leftTimeShiftQuery.close();
            rightTimeShiftQuery.close();
        }
        
        protected void calculateNewMedian(Group group) {
            if (this.isEven) {
                this.calculateNewMedianEven(group);
            } else {
                this.calculateNewMedianOdd(group);
            }
        }
        
        protected void calculateNewMedianEven(Group group) {
            if (group.getSumShift() == 0) {
                if (group.isLeftMedianIncluded() == false) {
                    // find new left neighbour for median
                    group.setNewMedian1(-1);
                    scanLeft++;
                }
                
                if (group.isRightMedianIncluded() == false) {
                    // find new right neighbour for median
                    group.setNewMedian2(1);
                    scanRight++;
                }
            } else if (group.getSumShift() == 1) {
                group.setNewMedianValue1(medianValue2);
                group.setHasNewMedian2(false);
            } else if (group.getSumShift() == 2) {
                group.setNewMedianValue1(medianValue2);
                
                group.setNewMedian2(1);
                scanRight++;
            } else if (group.getSumShift() > 0) {
                if (isEven(group.getSumShift())) {
                    int pos = group.getSumShift() / 2;
                    group.setNewMedian1(pos);
                    group.setNewMedian2(pos-1);
                    
                    scanRight += 2;
                } else {
                    int pos = (group.getSumShift()-1) / 2;
                    group.setNewMedian1(pos);
                    group.setHasNewMedian2(false);
                    scanRight++;
                }
            } else if (group.getSumShift() == -1) {
                group.setHasNewMedian2(false);
            } else if (group.getSumShift() == -2) {
                group.setNewMedian1(-1);
                scanLeft++;
                
                group.setHasNewMedian2(false);
                group.setNewMedianValue2(medianValue1);
            } else if (group.getSumShift() < 0) {
                if (isEven(group.getSumShift())) {
                    int pos = group.getSumShift() / 2;
                    group.setNewMedian1(pos);
                    group.setNewMedian2(pos+1);
                    
                    scanLeft += 2;
                } else {
                    int pos = (group.getSumShift() + 1) / 2;
                    group.setNewMedian1(pos);
                    group.setHasNewMedian2(false);
                    
                    scanLeft++;
                }
            }
        }
        
        protected void calculateNewMedianOdd(Group group) {
            if (group.getSumShift() == 0) {
                // no shift
                if (group.isLeftMedianIncluded() == false) {
                    // set left median to next left neighbour
                    group.setNewMedian1(-1);
                    scanLeft++;
                    
                    // set right median to next right neighbour
                    group.setNewMedian2(1);
                    scanRight++;
                }
            } else if (group.getSumShift() == -1) {
                group.setNewMedian1(-1);
                scanLeft++;
                
                group.setHasNewMedian2(false);
                group.setNewMedianValue2(medianValue1);
            } else if (group.getSumShift() == 1) {
                group.setNewMedian2(1);
                scanRight++;
                
                group.setNewMedianValue1(medianValue1);
            } else if (group.getSumShift() < 0) {
                if (isEven(group.getSumShift())) {
                    group.setNewMedian1(group.getSumShift() / 2);
                    group.setHasNewMedian2(false);
                    
                    scanLeft++;
                } else {
                    int newPos = (group.getSumShift()-1) / 2;
                    group.setNewMedian1(newPos);
                    group.setNewMedian2(newPos + 1);
                    
                    scanLeft += 2;
                }                
            } else if (group.getSumShift() > 0) {      
                if (isEven(group.getSumShift())) {
                    group.setNewMedian1(group.getSumShift() / 2);
                    group.setHasNewMedian2(false);
                    
                    scanRight++;
                } else {
                    int newPos = (group.getSumShift()+1) / 2;
                    group.setNewMedian1(newPos - 1);
                    group.setNewMedian2(newPos);
                    
                    scanRight += 2;
                }
            }
        }
        
        protected void compensateShift (Group group) {
            // don't compensate shift for groups that been failed to calculate shift for
            if (group.isSumShiftSet() == false) return;
            
            int sumShift = group.getSumShift();
                        
            // compensate shift for missing median (if so)
            if (isEven) {                    
                if (group.isLeftMedianIncluded() == false && sumShift < 0) {
                    sumShift += -2;
                } else if (group.isRightMedianIncluded() == false && sumShift > 0) {
                    sumShift += 2;
                }
                
                group.setNewMedianValue1(getMedianValue1());
                group.setNewMedianValue2(getMedianValue2());
            } else {
                if (group.isLeftMedianIncluded() == false) {
                    if (sumShift < 0) {
                        sumShift--;
                    } else if (sumShift > 0) {
                        sumShift++;
                    }
                }
                
                group.setNewMedianValue1(getMedianValue1());
            }
                        
            group.setSumShift(sumShift);
        }
        
        protected void loadMedianInfo () throws SQLException {
            PreparedStatement q = conn.prepareStatement("SELECT * FROM " + tableName + "_withvalues_median " +
                                                        " WHERE enabled != 0 AND start_time <= ? AND end_time >= ? ORDER BY row_count ASC LIMIT 1");

            q.setLong(1, startTime);
            q.setLong(2, endTime);

            ResultSet res = q.executeQuery();
            if (res.next() == false) {
                res.close();
                q.close();

                q = conn.prepareStatement("SELECT * FROM " + tableName + "_withvalues_median WHERE level = 1 AND part = 1");

                res = q.executeQuery();

                if (res.next() == false) {
                    res.close();
                    q.close();
                    throw new SQLException("Can't find any pre-calculated median info in median table!");
                }            
            }

            level = res.getInt("level");
            part = res.getInt("part");
            
            medianTime1 = res.getInt("median_time_1");
            medianStation1 = res.getInt("median_station_1");
            medianValue1 = res.getShort("median_temperature_1");
            
            medianTime2 = res.getInt("median_time_2");
            medianStation2 = res.getInt("median_station_2");
            medianValue2 = res.getShort("median_temperature_2");

            log("Using part #" + part + " of level " + level);

            isEven = (medianTime2 > 0);

            res.close();
            q.close();
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
                        int maxTime = minTime + (int)((double)(range + TIME_STEP)/groupPerStationCount);
                        
                        groupList[i][j] = new Group(this, minTime, maxTime, i+1, i+1);
                        groupRefList[(i*STATION_COUNT) + j] = groupList[i][j];
                        
                        minTime = maxTime;
                    }
                }
                
            } else {
                throw new UnsupportedOperationException("Groupcount smaller than 10 is not yet supported");
            }
        }
        
        public boolean assignRowValues(ResultSet receiver, int currentRow) throws SQLException {
            if (currentRow >= groupRefList.length) {
                return false;
            }
            
            Group currGroup = groupRefList[currentRow];
                        
            receiver.updateInt(1, currentRow);
            receiver.updateLong(2, currGroup.getStartTime());
            receiver.updateLong(3, currGroup.getEndTime());
            receiver.updateInt(4, currGroup.getStartStation());
            receiver.updateInt(5, currGroup.getEndStation());
            
            if (currGroup.isSumShiftSet()) {
                receiver.updateInt(6, currGroup.getSumShift());
            }
            receiver.updateDouble(7, currGroup.getMedian());
            
            return true;
        }

        public void close() throws SQLException {
            // not needed as this moment
        }
        
        public boolean isEven () {
            return this.isEven;
        }

        public short getMedianValue1() {
            return medianValue1;
        }

        public int getMedianTime1() {
            return medianTime1;
        }

        public int getMedianStation1() {
            return medianStation1;
        }

        public short getMedianValue2() {
            return medianValue2;
        }

        public int getMedianTime2() {
            return medianTime2;
        }

        public int getMedianStation2() {
            return medianStation2;
        }
        
        public boolean isEven (int num) {
            return (num % 2 == 0);
        }
        
    }
    
}
