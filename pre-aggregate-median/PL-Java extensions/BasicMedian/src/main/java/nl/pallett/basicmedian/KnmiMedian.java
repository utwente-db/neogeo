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
        
        // calculate medians
        provider.calculateMedians();
        
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
    
    public static class Group {
        protected int startTime;
        
        protected int endTime;
        
        protected int startStation;
        
        protected int endStation;
        
        protected int rowCount;
        
        protected double median;
        
        public Group (int startTime, int endTime, int startStation, int endStation) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.startStation = startStation;
            this.endStation = endStation;
        }
        
        public double getMedian () {
            return this.median;
        }
        
        public int getRowCount () {
            return this.rowCount;
        }
     
        public void determineMedian (PreparedStatement q) throws SQLException {
            q.setInt(1, getStartTime());
            q.setInt(2, getEndTime());
            q.setInt(3, getStartStation());
            q.setInt(4, getEndStation());
            
            ResultSet res = q.executeQuery();

            // collect all elements
            ArrayList<Integer> elements = new ArrayList<Integer>();
            while(res.next()) {
                elements.add(res.getInt(1));
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
    }
    
    public static class MedianProvider implements ResultSetProvider {
        public static final int STATION_COUNT = 10;
        
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
        
        public void calculateMedians () throws SQLException {
            determineGroups();
            determineMedians();
        }
        
        private void determineMedians () throws SQLException {
            PreparedStatement q = conn.prepareStatement("SELECT " + columnName + " FROM " + tableName + " WHERE timed >= ? AND timed < ? AND station >= ? AND station <= ?");
            
            for(Group[] groupSublist : groupList) {
                for(Group group : groupSublist) {
                    group.determineMedian(q);
                }
            }
            
            q.close();
        }
        
        private void determineGroups () {
            groupRefList = new Group[groupCount];
            
            if (groupCount == 1) {
                groupList = new Group[1][1];
                groupList[0][0] = new Group(startTime, endTime, startStation, endStation);
                groupRefList[0] = groupList[0][0];
            } else if (groupCount == STATION_COUNT) {
                groupList = new Group[STATION_COUNT][1];

                for(int i=startStation; i <= endStation; i++) {
                    groupList[i-1][0] = new Group(startTime, endTime, i, i);
                    groupRefList[i-1] = groupList[i-1][0];
                }               
            } else if (groupCount > STATION_COUNT) {
                int range = endTime - startTime;
                int groupPerStationCount = groupCount/STATION_COUNT;
                int step = range / groupPerStationCount;
                
                groupList = new Group[STATION_COUNT][groupPerStationCount];
                
                for(int i=startStation-1; i < endStation; i++) {
                    int minTime = startTime;
                    for(int j=0; j < groupPerStationCount; j++) {
                        int maxTime = minTime + step;
                        
                        groupList[i][j] = new Group(minTime, maxTime, i+1, i+1);
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
