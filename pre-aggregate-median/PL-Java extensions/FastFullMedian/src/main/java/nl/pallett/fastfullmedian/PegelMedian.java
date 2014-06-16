package nl.pallett.fastfullmedian;

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
        
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        
        MedianProvider provider = new MedianProvider(conn, tableName, startTime, endTime, groupCount);
        
        provider.calculateMedians();
        
        long execTime = System.nanoTime() - startExecTime;
        log("Median runtime: " + execTime + " ns"); 
        
        return provider;   
    }
        
    public static ResultSetProvider pegel_median (String tableName, int groupCount) throws SQLException {
        long startExecTime = System.nanoTime();  
        
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
        
        MedianProvider provider = new MedianProvider(conn, tableName, startTime, endTime, groupCount);
        provider.calculateMedians();
        
        long execTime = System.nanoTime() - startExecTime;
        log("Median runtime: " + execTime + " ns"); 
        
        return provider;        
    }   
    
    private static void log (String msg) {
        Logger.getAnonymousLogger().info(msg);
    }
    
    public static class Group {
        protected long startTime;
        
        protected long endTime;
        
        protected ArrayList<Long> timeList;
        
        protected double median;
        
        public Group (long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
            
            this.timeList = new ArrayList<Long>();
        }
        
        public int getRowCount () {
            return this.timeList.size();
        }
        
        public long getStartTime () {
            return this.startTime;
        }
        
        public long getEndTime () {
            return this.endTime;
        }
        
        public void addTime(long time) {
            this.timeList.add(time);
        }
        
        public void setMedian (double median) {
            this.median = median;
        }
        
        public double getMedian () {
            return this.median;
        }
        
        public long[] getMedianTimes () {
            long[] ret = {};
            
            if (timeList.size() > 0) {            
                if (timeList.size() % 2 == 0) {
                    int index1 = (int) (timeList.size() / 2) - 1;
                    int index2 = index1 + 1;

                    ret = new long[]{timeList.get(index1), timeList.get(index2)};
                } else {
                    int index = (int) Math.floor(timeList.size() / 2);
                    ret = new long[]{timeList.get(index)};
                }
            }
            
            return ret;
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
        
        protected DataInputStream dataStream = null;
    
        protected int dataStreamSize = 0;

        protected int dataStreamPos = 0;
               
        protected int level;
        
        protected int part;
        
        protected PreparedStatement fetchValueQuery;
        
        protected double groupDivisor;
        
        protected int indexMinus;
        
        protected double shiftTime = 0;
        
        protected ResultSet buffer;
        
        public MedianProvider (Connection conn, String tableName, long startTime, long endTime, int groupCount) {
            this.conn = conn;
            this.tableName = tableName;
            this.startTime = startTime;
            this.endTime = endTime;
            this.groupCount = groupCount;      
        }
        
        public void calculateMedians() throws SQLException {
            fetchValueQuery = conn.prepareStatement("SELECT PEGEL FROM " + tableName + " WHERE timed = ? LIMIT 1");
            
            calculateGroups();
            
            determineLevel();
            
            scanData();
            
            determineMedians();
            
            fetchValueQuery.close();
        }
        
        private void determineMedians () throws SQLException {
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
        }
        
        private void determineLevel () throws SQLException {
            // find the level/part the current range fits into
            PreparedStatement q = conn.prepareStatement("SELECT level, part FROM " + tableName + "_full_index WHERE start_time <= ? AND end_time >= ? "
                    + " AND enabled = 1 ORDER BY row_count ASC LIMIT 1");
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
                groupList[0] = new Group(startTime, endTime);
            } else {
                long range = (endTime + TIME_STEP) - startTime;
                this.groupDivisor = Math.floor(range / groupCount);
                this.indexMinus = (int) Math.floor( ((double)(startTime) / groupDivisor) );
                
                // This value is used to shift the timed values back so that the starttime gets value of 0.0000~ instead of e.g. 0.424
                shiftTime = (((double)(startTime) / groupDivisor ) - (double)indexMinus) * (double)groupDivisor;
                
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
                    
                    groupList[i] = new Group(minTime, maxTime);
                    
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
        }
        
    }
    

}
