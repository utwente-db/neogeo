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

public class PegelMedian {
    public static final boolean DEBUG = false;
    
    public static ResultSetProvider pegel_median (String tableName, long startTime, long endTime, int groupCount) throws SQLException {
        long startExecTime = System.nanoTime();  
        
        // setup object to calculate median
        MedianProvider provider = new MedianProvider(DriverManager.getConnection("jdbc:default:connection"), tableName, startTime, endTime, groupCount);
        
        provider.calculateMedians();
        
        long execTime = System.nanoTime() - startExecTime;
        log("Median runtime: " + execTime + " ns");  
        
        return provider;
    }
       
    public static ResultSetProvider pegel_median (String tableName, int groupCount) throws SQLException {        
        // fetch start and end time of table using median pre-generated structure
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement q = conn.prepareStatement("SELECT start_time, end_time FROM " + tableName + "_median WHERE level = 1");
        ResultSet res = q.executeQuery();
       
        if (res.next() == false) {
            throw new SQLException("Missing pre-generated Median structure for table " + tableName + " at level 1");
        }
       
        long startTime = res.getLong("start_time");
        long endTime = res.getLong("end_time") + 1;
        
        res.close();
        q.close();
        
        return pegel_median (tableName, startTime, endTime, groupCount);     
    }
    
    private static void log (String msg) {
        Logger.getAnonymousLogger().info(msg);
    }
    
    private static void logWarn (String msg) {
        Logger.getAnonymousLogger().warning(msg);
    }
    
    public static class Group {
        protected long startTime;
        
        protected long endTime;
        
        protected MedianProvider provider;
        
        protected int sumShift = -1;
        
        protected boolean sumShiftSet = false;
        
        protected boolean isLeftMedianIncluded = false;
        
        protected boolean isRightMedianIncluded = false;
                
        protected int newMedian1 = 0;
        
        protected int newMedian2 = 0;
        
        protected boolean hasNewMedian2 = false;
                
        protected int newMedianValue1 = -1;
        
        protected int newMedianValue2 = -1;
        
        protected double median = -1;
        
        protected int leftPos = 0;
        
        protected int rightPos = 0;
        
        public Group (MedianProvider provider, long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.provider = provider;
        }
        
        public long getStartTime () {
            return this.startTime;
        }
        
        public long getEndTime () {
            return this.endTime;
        }
        
        public double getMedian () {
            return this.median;
        }
        
        public void setMedian(double median) {
            this.median = median;
        }
        
        public int getSumShift () {
            return sumShift;
        }
        
        public void setSumShift(int sumShift) {
            this.sumShift = sumShift;
            sumShiftSet = true;
        }

        public boolean isSumShiftSet() {
            return sumShiftSet;
        }

        public boolean isLeftMedianIncluded() {
            return isLeftMedianIncluded;
        }

        public void setIsLeftMedianIncluded(boolean isLeftMedianIncluded) {
            this.isLeftMedianIncluded = isLeftMedianIncluded;
        }

        public boolean isRightMedianIncluded() {
            return isRightMedianIncluded;
        }

        public void setIsRightMedianIncluded(boolean isRightMedianIncluded) {
            this.isRightMedianIncluded = isRightMedianIncluded;
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
            this.newMedianValue2 = -1;
        }

        public int getNewMedianValue1() {
            return newMedianValue1;
        }

        public void setNewMedianValue1(int value) {
            this.newMedianValue1 = value;
        }

        public int getNewMedianValue2() {
            return newMedianValue2;          
        }

        public void setNewMedianValue2(int value) {
            this.newMedianValue2 = value;
        }
        
        public void addValue(int value, int dir) {
            if (dir < 0) {
                addValueLeft(value);
            } else {
                addValueRight(value);
            }            
        }
        
        protected void addValueLeft(int value) {
            leftPos--;
            
            if (newMedian1 == leftPos) {
                newMedianValue1 = value;
                provider.notifyLeft();
            } else if (hasNewMedian2 && newMedian2 == leftPos) {
                newMedianValue2 = value;
                provider.notifyLeft();
            }
        }
        
        protected void addValueRight(int value) {
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
        /*
         * This is the distance between two tuples for the time dimension
         */
        public static final int TIME_STEP = 600000;
        
        protected String tableName;
        
        protected long startTime;
        
        protected long endTime;
        
        protected int groupCount;
        
        protected Connection conn;
        
        protected Group[] groupList;
        
        protected long groupDivisor;
        
        protected int indexMinus;
        
        protected long shiftTime = 0;
        
        protected long medianTime1 = -1;
        
        protected long medianTime2 = -1;
        
        protected int medianValue1 = -1;
        
        protected int medianValue2 = -1;
        
        protected int level = -1;
        
        protected int part = -1;
        
        boolean isEven = false;
        
        protected PreparedStatement leftShiftQuery;
        
        protected PreparedStatement rightShiftQuery;
        
        protected int scanLeft = 0;
        
        protected int scanRight = 0;
        
        protected int scanRightTotal = 0;
        
        protected int scanLeftTotal = 0;
        
        protected DataInputStream leftStream = null;
    
        protected int leftStreamSize = 0;

        protected int leftStreamPos = 0;
        
        protected DataInputStream rightStream = null;
    
        protected int rightStreamSize = 0;

        protected int rightStreamPos = 0;
        
        protected long execStartTime = -1;
        
        protected long execTime = -1;
        
        protected ResultSet leftBuffer;
        
        protected ResultSet rightBuffer;
        
        public MedianProvider(Connection conn, String tableName, long startTime, long endTime, int groupCount) {
            this.conn = conn;
            this.tableName = tableName;
            this.startTime = startTime;
            this.endTime = endTime;
            this.groupCount = groupCount;  
        }
        
        public void notifyLeft () {
            scanLeft--;
        }
        
        public void notifyRight () {
            scanRight--;
        }
        
        public void calculateMedians () throws SQLException { 
            loadMedianInfo();
            calculateGroups();
            
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
            for (Group group : groupList) {
                if (group.isSumShiftSet() == false) continue;
                
                int newMedianValue1 = group.getNewMedianValue1();
                int newMedianValue2 = group.getNewMedianValue2();
                
                if (newMedianValue1 < 1) continue;
                
                double value1 = ((double)newMedianValue1) / 1000;
                
                double median = value1;
                if (newMedianValue2 > 0) {
                    double value2 = ((double)newMedianValue2) / 1000;
                    median = (value1 + value2) / 2;
                }
                
                group.setMedian(median);
            }
        }
        
        protected void scanData() throws SQLException {
            scanLeft();
            scanRight();
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
                
                long currTime = -1;
                int value = 0;
                try {
                    currTime = leftStream.readLong();
                    value = leftStream.readInt();
                } catch (IOException ex) {
                    throw new SQLException("IOException whilst trying to get next left neighbour");
                }
                leftStreamPos++;
                
                scanLeftTotal++;
                
                handleTimeValue(currTime, value, -1);
            }
            
            leftBuffer.close();
            q.close();
        }
        
        private void handleTimeValue (long timed, int value, int dir) {
            // ignore if it falls outside range
            if (timed < startTime || timed >= endTime) {
                return;
            }
                       
            // only a single group? then it's easy, just add to that group
            if (groupCount == 1) {
                this.groupList[0].addValue(value, dir);
            } else {           
                int index = (int) Math.floor(  ((double)(timed - shiftTime) / groupDivisor ) - indexMinus );
                                
                if (index >= 0 && index < groupList.length) {
                    this.groupList[index].addValue(value, dir);
                }
            }
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
        
        protected void scanRight () throws SQLException {            
            Statement q = conn.createStatement();
            rightBuffer = q.executeQuery("SELECT id, size, data FROM " + tableName + "_level" + level + "_data_withvalues WHERE direction = 1 AND part = 1 ORDER BY id ASC");
            
            while(scanRight > 0) {
                if (rightStream == null || rightStreamSize == rightStreamPos) {
                    if (getNextRightBuffer() == false) {
                        break;
                    }
                }
                
                long currTime = -1;
                int value = -1;
                try {
                    currTime = rightStream.readLong();
                    value = rightStream.readInt();
                } catch (IOException ex) {
                    throw new SQLException("IOException whilst trying to get next right neighbour");
                }
                rightStreamPos++;
                
                scanRightTotal++;
                
                handleTimeValue(currTime, value, 1);
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
        
        protected void calculateShifts () throws SQLException {
            leftShiftQuery = conn.prepareStatement("SELECT timed, shift_left AS shift FROM " + tableName + "_level" + level + "_shift" +
                                                   " WHERE part = " + this.part + " AND timed >= ? ORDER BY timed ASC LIMIT 1");
            rightShiftQuery = conn.prepareStatement("SELECT timed, shift_right AS shift FROM " + tableName + "_level" + level + "_shift" +
                                                   " WHERE part = " + this.part + " AND timed <= ? ORDER BY timed DESC LIMIT 1");
            
            for(Group group : groupList) {
                calculateShift(group);
                calculateNewMedian(group);
            }
            
            leftShiftQuery.close();
            rightShiftQuery.close();
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
            } else if (group.getSumShift() > 0) {
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
                
        protected void calculateShift (Group group) {
            int leftShift = -1;
            int rightShift = -1;
            
            try {
                ResultSet leftShiftRes = getShiftForTime(group.getStartTime(), "left");
                ResultSet rightShiftRes = getShiftForTime(group.getEndTime(), "right");
                
                leftShift = leftShiftRes.getInt("shift");
                rightShift = rightShiftRes.getInt("shift");
                
                // if right shift tuple is before left shift tuple
                // then range has no actual tuples and we ignore it in the rest of the calculations
                boolean hasNoTuples = (rightShiftRes.getLong("timed") < leftShiftRes.getLong("timed"));

                leftShiftRes.close();
                rightShiftRes.close();                    
                
                if (hasNoTuples) {
                    return;
                }
                
            } catch (SQLException e) {
                logWarn("Unable to determine sumShift for group: " + e.getMessage());
                return;
            }
            
            int sumShift = leftShift + rightShift;
            
            group.setIsLeftMedianIncluded((medianTime1 >= group.getStartTime() && medianTime1 < group.getEndTime()));
            group.setIsRightMedianIncluded(medianTime2 > -1 && medianTime2 >= group.getStartTime() && medianTime2 < group.getEndTime());
            
            // compensate shift for missing median (if so)
            if (isEven) {                    
                if (group.isLeftMedianIncluded() == false && sumShift < 0) {
                    sumShift += -2;
                } else if (group.isRightMedianIncluded() == false && sumShift > 0) {
                    sumShift += 2;
                }
                
                group.setNewMedianValue1(medianValue1);
                group.setNewMedianValue2(medianValue2);
            } else {
                if (group.isLeftMedianIncluded() == false) {
                    if (sumShift < 0) {
                        sumShift--;
                    } else if (sumShift > 0) {
                        sumShift++;
                    }
                }
                
                group.setNewMedianValue1(medianValue1);
            }
                        
            group.setSumShift(sumShift);
        }
        
        protected ResultSet getShiftForTime(long time, String shiftType) throws SQLException {
            ResultSet res;
            
            if (shiftType.equals("left")) {
                leftShiftQuery.setLong(1, time);
                res = leftShiftQuery.executeQuery();
            } else {
                rightShiftQuery.setLong(1, time);
                res = rightShiftQuery.executeQuery();
            }
            
            if (res.next() == false) {
                throw new SQLException("Unable to retrieve " + shiftType + " shift for tuple with time " + time);
            }

            return res;
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
            medianTime1 = res.getLong("median_time_1");
            medianTime2 = res.getLong("median_time_2");
            medianValue1 = res.getInt("median_value_1");
            medianValue2 = res.getInt("median_value_2");

            log("Using part #" + part + " of level " + level);

            isEven = (medianTime2 > 0);

            res.close();
            q.close();
        }
        
        private void calculateGroups () {
            if (groupCount == 1) {
                groupList = new Group[1];
                groupList[0] = new Group(this, startTime, endTime);
            } else {
                long range = endTime - startTime;
                this.groupDivisor = (range / groupCount);
                this.indexMinus = (int) Math.floor((double)startTime / groupDivisor );

                // This value is used to shift the timed values back so that the starttime gets value of 0.0000~ instead of e.g. 0.424
                shiftTime = (long)( (((double)startTime / groupDivisor ) - (double)indexMinus) * (double)groupDivisor );
                
                if (DEBUG) {
                    log("Start time: " + startTime);
                    log("Group divisor: " + groupDivisor);
                    log("indexMinus: " + indexMinus);
                    log("Shift time: " + shiftTime);
                }
                                                
                groupList = new Group[groupCount];
                
                long minTime = startTime;
                for (int i=0; i < groupCount; i++) {
                    long maxTime = minTime + (long)((double)(range + TIME_STEP)/groupCount);
                    if (maxTime > endTime) maxTime = endTime;
                    
                    groupList[i] = new Group(this, minTime, maxTime);
                    
                    minTime = maxTime;
                }
            }
        }

        @Override
        public boolean assignRowValues(ResultSet receiver, int currentRow) throws SQLException {
            if (currentRow >= groupList.length) {
                return false;
            }
            
            Group currGroup = groupList[currentRow];
                        
            receiver.updateInt(1, currentRow);
            receiver.updateLong(2, currGroup.getStartTime());
            receiver.updateLong(3, currGroup.getEndTime());
            
            if (currGroup.isSumShiftSet()) {
                receiver.updateInt(4, currGroup.getSumShift());
            }
            receiver.updateDouble(5, currGroup.getMedian());
            
            return true;
        }
        
        public boolean isEven (int num) {
            return (num % 2 == 0);
        }

        @Override
        public void close() throws SQLException {
        }
    }
    

}