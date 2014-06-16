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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.pljava.ResultSetProvider;

public class PegelMedian {
    
    public static ResultSetProvider pegel_median (String tableName, long startTime, long endTime, int groupCount) throws SQLException {
        long startExecTime = System.nanoTime();  

        // create new Median Provider
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        MedianProvider provider = new MedianProvider(conn, tableName, startTime, endTime, groupCount);
        
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
    
    public static ResultSetProvider pegel_median (String tableName, int groupCount) throws SQLException {        
        // find start and end time of table
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        
        PreparedStatement q = conn.prepareStatement("SELECT start_time, end_time FROM " + tableName + "_full_index WHERE level = 1 AND part = 1");
        ResultSet res = q.executeQuery();
        
        if (res.next() == false) {
            throw new SQLException("Unable to find start and end time in index table");
        }
        
        long startTime = res.getLong("start_time");
        long endTime = res.getLong("end_time") + 1;
        
        res.close();
        q.close();
        
        return pegel_median(tableName, startTime, endTime, groupCount);
    }   
    
    private static void log (String msg) {
        Logger.getAnonymousLogger().info(msg);
    }
    
    public static class Group {
        protected long startTime;
        
        protected long endTime;
        
        protected int rowCount;
        
        protected double median;
        
        protected long medianTime1;
        
        protected long medianTime2;
        
        protected int medianPos1 = -1;
        
        protected int medianPos2 = -1;
        
        protected int currPos = 0;
        
        protected MedianProvider provider;
                
        public Group (MedianProvider provider, long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.provider = provider;
        }
        
        public int getRowCount () {
            return this.rowCount;
        }
        
        public void setRowCount(int rowCount) {
            this.rowCount = rowCount;
            
            if (rowCount > 0) {
                if (rowCount % 2 == 0) {
                    medianPos1 = (int) (rowCount/2) - 1;
                    medianPos2 = medianPos1 + 1;
                } else {
                    medianPos1 = (int) Math.floor(rowCount / 2);
                }
            }
        }
        
        public long getStartTime () {
            return this.startTime;
        }
        
        public long getEndTime () {
            return this.endTime;
        }
        
        public void addTime(long time) {
            if (currPos == medianPos1) {
                medianTime1 = time;
                
                if (medianPos2 == -1) {
                    provider.notifyFoundMedian();
                }
            } else if (currPos == medianPos2) {
                medianTime2 = time;
                provider.notifyFoundMedian();
            }
            
            currPos++;
        }
        
        public void setMedian (double median) {
            this.median = median;
        }
        
        public double getMedian () {
            return this.median;
        }
        
        public long[] getMedianTimes () {
            long[] ret = {};
            
            if (medianPos1 > -1 && medianPos2 > -1) {
                ret = new long[]{medianTime1, medianTime2};
            } else if (medianPos1 > -1) {
                ret = new long[]{medianTime1};
            }
            
            return ret;
        }
    }
    
    public static class MedianProvider implements ResultSetProvider {
        /*
         * This is the distance between two tuples for the time dimension
         */
        public static final int TIME_STEP = 600;
        
        protected String tableName;
        
        protected long startTime;
        
        protected long endTime;
        
        protected int groupCount;
        
        protected Connection conn;
        
        protected Group[] groupList;
        
        protected DataInputStream dataStream = null;
    
        protected int dataStreamSize = 0;

        protected int dataStreamPos = 0;
                
        protected int level;
        
        protected int part;
        
        protected PreparedStatement fetchValueQuery;
        
        protected double groupDivisor;
        
        protected int indexMinus;
        
        protected double shiftTime = 0;
        
        protected int mediansFoundCount = 0;
        
        protected ResultSet buffer;
                
        public MedianProvider (Connection conn, String tableName, long startTime, long endTime, int groupCount) {
            this.conn = conn;
            this.tableName = tableName;
            this.startTime = startTime;
            this.endTime = endTime;
            this.groupCount = groupCount;         
        }
        
        public void notifyFoundMedian () {
            mediansFoundCount++;
        }
        
        public void countRows () throws SQLException {
            PreparedStatement countQuery = conn.prepareStatement("SELECT COUNT(*) AS row_count FROM " + tableName + " WHERE timed >= ? AND timed < ?");
            
            // determine number of rows for each group
            for(Group group : groupList) {
                countQuery.setLong(1, group.getStartTime());
                countQuery.setLong(2, group.getEndTime());
                
                ResultSet res = countQuery.executeQuery();
                res.next();
                
                group.setRowCount(res.getInt("row_count"));
                
                res.close();
                countQuery.close();
            }
        }
        
        public void prepare() throws SQLException {
            calculateGroups();
            determineLevel();
        }
        
        public void calculateMedians() throws SQLException {
            scanData();
            determineMedians();
        }
        
        private void determineMedians () throws SQLException {
            fetchValueQuery = conn.prepareStatement("SELECT PEGEL FROM " + tableName + " WHERE timed = ? LIMIT 1");
            
            for(Group group : groupList) {
                long[] medianTimes = group.getMedianTimes();
                double median = 0;

                if (medianTimes.length == 2) {
                    median = (fetchValueForTime(medianTimes[0]) + fetchValueForTime(medianTimes[1])) / 2;
                } else if (medianTimes.length == 1) {
                    median = fetchValueForTime(medianTimes[0]);
                }
                
                group.setMedian(median);
            }
            
            fetchValueQuery.close();
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
                    long timed = dataStream.readLong();
                    handleTimeValue(timed);
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
        
        private void handleTimeValue (long timed) {
            // ignore if it falls outside range
            if (timed < startTime || timed >= endTime) {
                return;
            }
             
            // only a single group? then it's easy, just add to that group
            if (groupCount == 1) {
                this.groupList[0].addTime(timed);
            } else {           
                int index = (int) Math.floor(((double)timed - shiftTime) / groupDivisor ) - indexMinus;
                
                if (index >= 0 && index < groupList.length) {
                    this.groupList[index].addTime(timed);
                }
            }
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
        
        private void calculateGroups () {
            if (groupCount == 1) {
                groupList = new Group[1];
                groupList[0] = new Group(this, startTime, endTime);
            } else {
                long range = (endTime + TIME_STEP) - startTime;
                this.groupDivisor = Math.floor(range / groupCount);
                this.indexMinus = (int) Math.floor((double)startTime / groupDivisor );
                
                // This value is used to shift the timed values back so that the starttime gets value of 0.0000~ instead of e.g. 0.424
                shiftTime = (((double)startTime / groupDivisor ) - (double)indexMinus) * (double)groupDivisor;
                
                /*
                log("Group divisor: " + groupDivisor);
                log("Range: " + range);
                log("IndexMinus: " + indexMinus);
                log("Shift timed: " + shiftTime);
                */
                                
                groupList = new Group[groupCount];
                
                long minTime = startTime;
                for (int i=0; i < groupCount; i++) {
                    long maxTime = minTime + (long)groupDivisor - TIME_STEP;
                    
                    groupList[i] = new Group(this, minTime, maxTime);
                    
                    minTime = maxTime + TIME_STEP;
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
            receiver.updateInt(2, currGroup.getRowCount());
            receiver.updateLong(3, currGroup.getStartTime());
            receiver.updateLong(4, currGroup.getEndTime());
            receiver.updateDouble(5, currGroup.getMedian());
            
            return true;
        }
        
        protected double fetchValueForTime(long time) throws SQLException {            
            fetchValueQuery.setLong(1, time);
            ResultSet res = fetchValueQuery.executeQuery();
           
            if (res.next() == false) {
                log("Unable to fetch value for time " + time);
                return -1;
            }
           
            double value = res.getDouble("PEGEL");           
            res.close();           
           
            return value;
        }
    

        @Override
        public void close() throws SQLException {
            // not needed yet
        }
        
    }
    

}
