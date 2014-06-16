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
public class TwitterMedian {
    public static final boolean debug = true;
    
    public static ResultSetProvider twitter_median (String tableName, String columnName, int startTime, int endTime, 
            double minX, double maxX, double minY, double maxY, int groupCount) throws SQLException {
        long startExecTime = System.nanoTime();  

        // create new Median Provider
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        MedianProvider provider = new MedianProvider(conn, tableName, columnName, startTime, endTime, minX, maxX, minY, maxY, groupCount);
        
        // calculate medians
        provider.calculateMedians();
        
        long execTime = System.nanoTime() - startExecTime;
        log("Median runtime: " + execTime + " ns");        
        
        return provider;
    }
        
    public static ResultSetProvider twitter_median (String tableName, String columnName, int groupCount) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        
        PreparedStatement q = null;
        if (tableName.toLowerCase().endsWith("_xy")) {        
            q = conn.prepareStatement("SELECT MIN(timed) AS start_time, MAX(timed) AS end_time, "
                    + "MIN(x_coordinate) AS min_x, MAX(x_coordinate) AS max_x, "
                    + "MIN(y_coordinate) AS min_y, MAX(y_coordinate) AS max_y "
                    + "FROM " + tableName);
        } else {
            q = conn.prepareStatement("SELECT MIN(timed) AS start_time, MAX(timed) AS end_time, "
                    + "ST_XMin(ST_Extent(coordinates)) AS min_x, ST_XMax(ST_Extent(coordinates)) AS max_x, "
                    + "ST_YMin(ST_Extent(coordinates)) AS min_y, ST_YMax(ST_Extent(coordinates)) AS max_y "
                    + "FROM " + tableName);
        }
        
        ResultSet res = q.executeQuery();
        
        if (res.next() == false) {
            throw new SQLException("Unable to determine limits of table");
        }
        
        int startTime = res.getInt("start_time");
        int endTime = res.getInt("end_time") + 1;
        
        double minX = res.getDouble("min_x");
        double maxX = res.getDouble("max_x") + 1;
        double minY = res.getDouble("min_y");
        double maxY = res.getDouble("max_y") + 1;
        
        res.close();
        q.close();
        
        return twitter_median (tableName, columnName, startTime, endTime, minX, maxX, minY, maxY, groupCount);
    }
    
    private static void log (String msg) {
        Logger.getAnonymousLogger().info(msg);
    }
    
    public static class Group {
        protected int startTime;
        
        protected int endTime;
        
        protected double minX;
        
        protected double maxX;
        
        protected double minY;
        
        protected double maxY;
        
        protected int rowCount;
        
        protected double median;
        
        public Group (int startTime, int endTime, double minX, double maxX, double minY, double maxY) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
        }
        
        public double getMedian () {
            return this.median;
        }
        
        public int getRowCount () {
            return this.rowCount;
        }
        
        public String getBoundingBoxAsWkt () {
            return "POLYGON((" + 
                    minX + " " + minY + "," +
                    maxX + " " + minY + "," +
                    maxX + " " + maxY + "," +
                    minX + " " + maxY + "," +
                    minX + " " + minY + "" +
                    "))";                    
        }
     
        public void determineMedian (boolean isGis, PreparedStatement q) throws SQLException {
            q.setInt(1, getStartTime());
            q.setInt(2, getEndTime());
            
            if (isGis == false) {               
                q.setDouble(3, minX);
                q.setDouble(4, maxX);
                q.setDouble(5, minY);
                q.setDouble(6, maxY);
            }
            
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

        public double getMinX() {
            return minX;
        }

        public double getMaxX() {
            return maxX;
        }

        public double getMinY() {
            return minY;
        }

        public double getMaxY() {
            return maxY;
        }
    }
    
    public static class MedianProvider implements ResultSetProvider {        
        public static final int SRID = 4326;
        
        protected String tableName;
        
        protected String columnName;
        
        protected int startTime;
        
        protected int endTime;
        
        protected double minX;
        
        protected double maxX;
        
        protected double minY;
        
        protected double maxY;
        
        protected int groupCount;
        
        protected Connection conn;
        
        protected Group[] groupList;
        
        protected boolean isGis;
        
        public MedianProvider(Connection conn, String tableName, String columnName, int startTime, int endTime, 
                double minX, double maxX, double minY, double maxY, int groupCount) {
            this.conn = conn;
            this.tableName = tableName;
            this.columnName = columnName;
            this.startTime = startTime;
            this.endTime = endTime;
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
            this.groupCount = groupCount;  
            this.isGis = (tableName.toLowerCase().endsWith("_xy") == false);
        }
        
        public void calculateMedians () throws SQLException {
            determineGroups();
            determineMedians();
        }
        
        private void determineMedians () throws SQLException {
            PreparedStatement q = null;
            
            if (isGis == false) {           
                q = conn.prepareStatement("SELECT " + columnName + " FROM " + tableName + " WHERE timed >= ? AND timed < ? "
                        + " AND x_coordinate >= ? AND x_coordinate < ?"
                        + " AND y_coordinate >= ? AND y_coordinate < ?");
            }
            
            for(Group group : groupList) {
                if (isGis) {
                    q = conn.prepareStatement("SELECT " + columnName + " FROM " + tableName + " WHERE timed >= ? AND timed < ?"
                        + " AND ST_Intersects(coordinates, ST_GeomFromText('" + group.getBoundingBoxAsWkt() + "', " + SRID + "))");
                }
                
                group.determineMedian(isGis, q);
                
                if (isGis) {
                    q.close();
                }
            }
            
            if (isGis == false) {
                q.close();
            }
        }
        
        protected void determineGroups () throws SQLException {
            groupList = new Group[groupCount];
            
            if (groupCount == 1) {
                groupList[0] = new Group(startTime, endTime, minX, maxX, minY, maxY);
            } else if (groupCount == 10) {
                // group x in 5 and y into 2 to generate a 5x2 grid
                this.calculateGroups(5, 2);
            } else if (groupCount == 100) {
                // group x in 10 and y into 10 to generate a 10x10 grid
                this.calculateGroups(10, 10);
            } else {
                throw new SQLException("Groupcount of " + groupCount + " not yet supported!");
            }
        }
        
        protected void calculateGroups (int groupsX, int groupsY) {
            double IntervalX = maxX - minX;
            double IntervalY = maxY - minY;
            
            double stepX = IntervalX / groupsX;
            double stepY = IntervalY / groupsY;
                  
            double currMinX = this.minX;
            int groupId = 0;
            for(int i=0; i < groupsX; i++) {
                double currMaxX = currMinX + stepX;
                
                double currMinY = this.minY;
                for (int j=0; j < groupsY; j++) {
                    double currMaxY = currMinY + stepY;                    
                    
                    groupList[groupId] = new Group(startTime, endTime, currMinX, currMaxX, currMinY, currMaxY);
                    groupId++;                    
                    
                    currMinY = currMaxY;
                }
                
                currMinX = currMaxX;
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
            receiver.updateDouble(5, currGroup.getMinX());
            receiver.updateDouble(6, currGroup.getMaxX());
            receiver.updateDouble(7, currGroup.getMinY());
            receiver.updateDouble(8, currGroup.getMaxY());
            receiver.updateDouble(9, currGroup.getMedian());
            
            return true;
        }

        public void close() throws SQLException {
            // not needed yet
        }
        
    }
    
}
