/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.basicmedian;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Logger;
import org.postgresql.pljava.ResultSetProvider;

/**
 *
 * @author
 * Dennis
 */
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
                
        protected int rowCount;
        
        protected double median;
        
        public Group (long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }
        
        public double getMedian () {
            return this.median;
        }
        
        public int getRowCount () {
            return this.rowCount;
        }
     
        public void determineMedian (PreparedStatement q) throws SQLException {
            q.setLong(1, getStartTime());
            q.setLong(2, getEndTime());
            
            ResultSet res = q.executeQuery();

            // collect all elements
            ArrayList<Double> elements = new ArrayList<Double>();
            while(res.next()) {
                elements.add(res.getDouble(1));
            }          
            
            res.close();
            
            // store number of elements collected
            this.rowCount = elements.size();
            
            // sort collected elements
            if (elements.size() > 1) {
                Collections.sort(elements);
            }
            
             // calculate median
            if (elements.size() > 0) {
                if (elements.size() % 2 == 0) {
                    int index1 = (int) (elements.size() / 2) - 1;
                    int index2 = index1 + 1;

                    median = ((double)elements.get(index1) + (double)elements.get(index2)) / 2;
                } else {
                    int index = (int) Math.floor(elements.size() / 2);
                    median = (double) elements.get(index);
                }
            }
            
            // make sure elements are removed from memory
            elements.clear();
            elements = null;            
        }

        /**
         * @return the startTime
         */
        public long getStartTime() {
            return startTime;
        }

        /**
         * @return the endTime
         */
        public long getEndTime() {
            return endTime;
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
        
        protected double groupDivisor;
        
        protected int indexMinus;
        
        protected double shiftTime = 0;
        
        public MedianProvider(Connection conn, String tableName, long startTime, long endTime, int groupCount) {
            this.conn = conn;
            this.tableName = tableName;
            this.startTime = startTime;
            this.endTime = endTime;
            this.groupCount = groupCount;  
        }
        
        public void calculateMedians () throws SQLException {
            calculateGroups();            
            determineMedians();
        }
        
        private void determineMedians () throws SQLException {
            PreparedStatement q = conn.prepareStatement("SELECT PEGEL FROM " + tableName + " WHERE timed >= ? AND timed < ?");
            
            for(Group group : groupList) {
                group.determineMedian(q);
            }
            
            q.close();
        }
        
        private void calculateGroups () {
            if (groupCount == 1) {
                groupList = new Group[1];
                groupList[0] = new Group(startTime, endTime);
            } else {
                long range = (endTime + TIME_STEP) - startTime;
                this.groupDivisor = Math.floor(range / groupCount);
                                
                groupList = new Group[groupCount];
                
                long minTime = startTime;
                for (int i=0; i < groupCount; i++) {
                    long maxTime = minTime + (long)groupDivisor;
                    
                    groupList[i] = new Group(minTime, maxTime);
                    
                    minTime = maxTime;
                }
            }
        }

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

        public void close() throws SQLException {
            // not needed yet
        }
        
        
    }
}
