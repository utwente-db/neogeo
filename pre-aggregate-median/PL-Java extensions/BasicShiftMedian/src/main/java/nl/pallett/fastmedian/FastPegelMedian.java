package nl.pallett.fastmedian;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FastPegelMedian {
    protected Connection conn;
    
    protected String tableName;
    
    protected long startTime;
    
    protected long endTime;
    
    protected int level;
    
    protected int part;
    
    protected long medianTime1 = 0;
    
    protected long medianTime2 = 0;
    
    protected boolean isEven = false;
    
    protected double median = 0;
    
    protected long newMedianTime1 = 0;
    
    protected long newMedianTime2 = 0;

    protected DataInputStream rightStream = null;
    
    protected int rightStreamSize = 0;
    
    protected int rightStreamPos = 0;
       
    protected DataInputStream leftStream = null;
    
    protected int leftStreamSize = 0;
    
    protected int leftStreamPos = 0;
    
    protected int currLeftId = -1;
    
    protected int currRightId = -1;
            
    public static double pegel_median (String tableName, long startTime, long endTime) throws SQLException {
        long startExecTime = System.nanoTime();  
        
        // setup object to calculate median
        FastPegelMedian obj = new FastPegelMedian(DriverManager.getConnection("jdbc:default:connection"), tableName);
        
        // use object to calculate median
        double median = obj.getMedian(startTime, endTime);
        
        long execTime = System.nanoTime() - startExecTime;
        log("Median runtime: " + execTime + " ns");        
        return median;
    }
       
    public static double pegel_median (String tableName) throws SQLException {
        long startExecTime = System.nanoTime();  
        
        // fetch start and end time of table using median pre-generated structure
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement q = conn.prepareStatement("SELECT start_time, end_time FROM " + tableName + "_median WHERE level = 1");
        ResultSet res = q.executeQuery();
       
        if (res.next() == false) {
            throw new SQLException("Missing pre-generated Median structure for table " + tableName + " at level 1");
        }
       
        long startTime = res.getLong("start_time");
        long endTime = res.getLong("end_time");
        
        res.close();
        q.close();
                
        // setup object to calculate median
        FastPegelMedian obj = new FastPegelMedian(conn, tableName);
        
        // use object to calculate median
        double median = obj.getMedian(startTime, endTime);
        
        long execTime = System.nanoTime() - startExecTime;
        log("Median runtime: " + execTime + " ns");        
        return median;
    }
    
    private static void log (String msg) {
        Logger.getAnonymousLogger().info(msg);
    }
    
    private static void logWarn (String msg) {
        Logger.getAnonymousLogger().warning(msg);
    }
    
    public FastPegelMedian(Connection conn, String tableName) throws SQLException {
        this.conn = conn;
        this.tableName = tableName; 
    }
    
    protected void loadMedianInfo () throws SQLException {
        PreparedStatement q = conn.prepareStatement("SELECT level, part, median_time_1, median_time_2 FROM " + tableName + "_median " +
                                                    " WHERE enabled != 0 AND start_time <= ? AND end_time >= ? ORDER BY row_count ASC LIMIT 1");

        q.setLong(1, startTime);
        q.setLong(2, endTime);
        
        ResultSet res = q.executeQuery();
        if (res.next() == false) {
            throw new SQLException("Can't find any pre-calculated median info in median table!");
        }
        
        level = res.getInt("level");
        part = res.getInt("part");
        medianTime1 = res.getLong("median_time_1");
        medianTime2 = res.getLong("median_time_2");
        
        log("Using part #" + part + " of level " + level);
        
        isEven = (medianTime2 > 0);
        
        res.close();
        q.close();
    }
    
    public double getMedian(long startTime, long endTime) throws SQLException {
        this.startTime = startTime;
        this.endTime = endTime;
        
         // load median information
        try {
            this.loadMedianInfo();
        } catch (SQLException e) {
            throw new SQLException("Unable to load pre-calculated median info for table " + tableName);
        }
        
        // calculate total shift
        int leftShift = -1;
        int rightShift = -1;
        try {
            leftShift = getShiftForTime(startTime, "left");
            rightShift = getShiftForTime(endTime, "right");
        } catch (SQLException e) {
            logWarn(e.getMessage());
            return -1;
        }
 
        
        int sumShift = leftShift + rightShift;
                       
        if (isEven) {
            this.calculateMedianEven(sumShift);
        } else {
            this.calculateMedianOdd(sumShift);
        }
        
        // have we been able to determine the new median?
        if (newMedianTime1 == 0) {
            return -1;
        }
               
        // determine new median
        double value1 = getPegelValueForTime(newMedianTime1);
        double median = value1;
        if (newMedianTime2 > 0) {
            double value2 = getPegelValueForTime(newMedianTime2);
            median = ((value1 + value2) / 2);
        }
        
        return median;
    }
    
    protected void calculateMedianOdd (int sumShift) throws SQLException {
        // is dataset median part of range?
        boolean isMedianIncluded = (medianTime1 >= this.startTime && medianTime1 <= this.endTime);
        
        if (sumShift == 0) {
            this.calculateMedianOddNoShift(isMedianIncluded);
        } else {
            this.calculateMedianOddWithShift(sumShift, isMedianIncluded);
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
        long[] currSet = {medianTime1};
        
        // calculate new median
        calculateMedianWithShift (sumShift, currSet);
    }
    
    protected void calculateMedianOddNoShift(boolean isMedianIncluded) throws SQLException {
        if (isMedianIncluded) {
            this.newMedianTime1 = medianTime1;
            return;
        } else {
            // split median into its nearest neighbours
            // find new left neighbour for median
            this.newMedianTime1 = getNextLeftTime();
            
            // find new left neihbour for median
            this.newMedianTime2 = getNextRightTime();
        }
    }    
    
    protected void calculateMedianEven (int sumShift) throws SQLException {
        boolean isLeftMedianIncluded = (medianTime1 >= this.startTime && medianTime1 <= this.endTime);
        boolean isRightMedianIncluded = (medianTime2 >= this.startTime && medianTime2 <= this.endTime);
        
        if (sumShift == 0) {
            this.calculateMedianEvenNoShift(isLeftMedianIncluded, isRightMedianIncluded);
        } else {
            this.calculcateMedianEvenWithShift(sumShift, isLeftMedianIncluded, isRightMedianIncluded);
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
        long[] currSet = {medianTime1, medianTime2};
        
        // calculate new median
        calculateMedianWithShift (sumShift, currSet);
    }

    protected void calculateMedianEvenNoShift(boolean isLeftMedianIncluded, boolean isRightMedianIncluded) throws SQLException {
        // if both left and right median are included
        // and no shift then new median is exactly the current median
        if (isLeftMedianIncluded && isRightMedianIncluded) {
            this.newMedianTime1 = medianTime1;
            this.newMedianTime2 = medianTime2;
            return;
        }
        
        if (isLeftMedianIncluded == false) {
            // find new left neighbour for median
            this.newMedianTime1 = getNextLeftTime();
        }
        
        if (isRightMedianIncluded == false) {
            // find new left neihbour for median
            this.newMedianTime2 = getNextRightTime();
        }        
    }
    
    protected void calculateMedianWithShift (int sumShift, long[] currSet) throws SQLException {        
        if (sumShift < 0) {
            while (sumShift < 0) {
                // current median set consists of two parts?
                // then move 1 to the left by taking left part of current median
                if (currSet.length == 2) {
                    currSet = new long[]{currSet[0]};
                    sumShift++;                    
                } else {
                    // find next left neighbour
                    long nextTime = getNextLeftTime();
                    currSet = new long[]{nextTime, currSet[0]};
                    sumShift++;
                }  
            }
        } else {
            while(sumShift > 0) {
                // current median set consists of two parts?
                // then move 1 to the right by taking right part of current median
                if (currSet.length == 2) {
                    currSet = new long[]{currSet[1]};
                    sumShift--;                    
                } else {
                    // find next right neighbour
                    long nextTime = getNextRightTime();
                    currSet = new long[]{currSet[0], nextTime};
                    sumShift--;
                }              
            }            
        }
        
        if (currSet.length == 2) {
            newMedianTime1 = currSet[0];
            newMedianTime2 = currSet[1];
        } else {
            newMedianTime1 = currSet[0];
        }
    }
    
    protected long getNextRightTime () throws SQLException{
        if (rightStream == null || rightStreamSize == rightStreamPos) {
            if (getNextRightBuffer() == false) {
                return 0;
            }
        }

        long nextTime = 0;
        
        while(nextTime == 0) {
            long currTime = -1;
            try {
                currTime = rightStream.readLong();
            } catch (IOException ex) {
                throw new SQLException("IOException whilst trying to get next right neighbour");
            }
            rightStreamPos++;
                        
            if (currTime >= this.startTime && currTime <= this.endTime) {
                nextTime = currTime;
                break;
            }        
            
            if (rightStreamSize == rightStreamPos) {
                if (getNextRightBuffer() == false) {
                    break;
                }
            }
        }
        
        return nextTime;
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
    
    protected long getNextLeftTime () throws SQLException{
        if (leftStream == null || leftStreamSize == leftStreamPos) {
            if (getNextLeftBuffer() == false) {
                return 0;
            }
        }

        long nextTime = 0;
        
        while(nextTime == 0) {
            long currTime = -1;
            try {
                currTime = leftStream.readLong();
            } catch (IOException ex) {
                throw new SQLException("IOException whilst trying to get next left neighbour");
            }
            leftStreamPos++;
                        
            if (currTime >= this.startTime && currTime <= this.endTime) {
                nextTime = currTime;
                break;
            }        
            
            if (leftStreamSize == leftStreamPos) {
                if (getNextLeftBuffer() == false) {
                    break;
                }
            }
        }
        
        return nextTime;
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
    
    protected int getShiftForTime(long time, String shiftType) throws SQLException {
        String sql = "SELECT shift_" + shiftType + " AS shift FROM " + tableName + "_level" + level + "_shift WHERE part = " + this.part + " AND ";
        if (shiftType.equalsIgnoreCase("left")) {
            sql += "timed > " + (time-1) + " ORDER BY timed ASC";
        } else {
            sql += "timed < " + (time+1) + " ORDER BY timed DESC";
        }
        sql += " LIMIT 1";
        
        PreparedStatement q = conn.prepareStatement(sql);        
        ResultSet res = q.executeQuery();
        
        if (res.next() == false) {
            throw new SQLException("Unable to retrieve " + shiftType + " shift for tuple with time " + time);
        }
                
        int shift = res.getInt("shift");
        
        res.close();
        q.close();
        
        return shift;
    }
    
    protected double getPegelValueForTime(long time) throws SQLException {
        PreparedStatement q = conn.prepareStatement("SELECT PEGEL FROM " + tableName + " WHERE timed = ?");
        q.setLong(1, time);
        
        ResultSet res = q.executeQuery();
        if (res.next() == false) {
            logWarn("Unable to retrieve PEGEL value for timed " + time);
            return 0;
        }
        
        double value = res.getDouble("PEGEL");
        
        res.close();
        q.close();
        return value;
    }
           
    public static void main (String[] args) throws Exception {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            throw new SQLException("JDBC Driver cannot be initialized");
        }

        String url = "jdbc:postgresql://localhost/pegel";
        Properties props = new Properties();
        props.setProperty("user", "postgres");
        props.setProperty("password", "sa");
        
        Connection conn = DriverManager.getConnection(url, props);
        
        String startTime = "1167606600000";
        String endTime = "1173604800000";
        
        //startTime = "1203986046000";
        //endTime = "1205734086000";
        
        FastPegelMedian obj = new FastPegelMedian(conn, "pegel_100k");
        double median = obj.getMedian(Long.parseLong(startTime), Long.parseLong(endTime));
        System.out.println("New median is: " + median);
        
        conn.close();

    }
    

}