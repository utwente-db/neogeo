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
public class TwitterMedian {
    public static final boolean DEBUG = false;
    
    public static ResultSetProvider twitter_median (String tableName, String columnName, int startTime, int endTime, 
            double minX, double maxX, double minY, double maxY, int groupCount) throws SQLException {
        long startExecTime = System.nanoTime();  

        // create new Median Provider
        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        MedianProvider provider = new MedianProvider(conn, tableName, columnName, startTime, endTime, minX, maxX, minY, maxY, groupCount);
        
        provider.prepare();
        
        // stop timer
        long execTime1 = System.nanoTime() - startExecTime;
        
        // do "off-the-clock" counting here
        // why? because we do not consider the counting to be part of our timed results
        provider.countRows();
        
        // start timer again
        startExecTime = System.nanoTime();

        provider.calculateMedians();
        
        // output final execution time
        long execTime2 = System.nanoTime() - startExecTime;
        long execTime = execTime1 + execTime2;
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
    
    private static void logWarn (String msg) {
        Logger.getAnonymousLogger().warning(msg);
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
        
        protected long medianRowId1;
        
        protected long medianRowId2;
        
        protected int medianPos1 = -1;
        
        protected int medianPos2 = -1;
        
        protected int currPos = 0;
        
        protected MedianProvider provider;
                
        public Group (MedianProvider provider, int startTime, int endTime, double minX, double maxX, double minY, double maxY) {
            this.provider = provider;
            this.startTime = startTime;
            this.endTime = endTime;
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
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
        
        public long[] getMedianRowIds () {
            long[] ret = {};
            
            if (medianPos1 > -1 && medianPos2 > -1) {
                ret = new long[]{medianRowId1, medianRowId2};
            } else if (medianPos1 > -1) {
                ret = new long[]{medianRowId1};
            }
            
            return ret;
        }

        public void addTuple (long rowId) {            
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
        
        public static final int TIME_STEP = 1;
        
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
        
        protected Group[] groupRefList;     
        
        protected Group[][] groupList;
        
        protected PreparedStatement fetchValueQuery;
        
        protected int level;
        
        protected int part;
        
        protected DataInputStream dataStream = null;
    
        protected int dataStreamSize = 0;

        protected int dataStreamPos = 0;
               
        protected double groupDivisor;
        
        protected int indexMinus;
        
        protected double shiftTime;
        
        protected ResultSet buffer;
        
        protected int indexMinusX;
        
        protected int indexMinusY;
        
        protected double shiftX;
        
        protected double shiftY;
        
        protected double stepX;
        
        protected double stepY;
        
        protected int mediansFoundCount = 0;
        
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
                    + "AND x_coordinate >= ? AND x_coordinate < ?"
                    + "AND y_coordinate >= ? AND y_coordinate < ?");
            
            // determine number of rows for each group
            for(Group group : groupRefList) {
                countQuery.setInt(1, group.getStartTime());
                countQuery.setInt(2, group.getEndTime());
                countQuery.setDouble(3, group.getMinX());
                countQuery.setDouble(4, group.getMaxX());
                countQuery.setDouble(5, group.getMinY());
                countQuery.setDouble(6, group.getMaxY());
                
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
            fetchValueQuery = conn.prepareStatement("SELECT " + columnName + " FROM " + tableName + " WHERE id = ? LIMIT 1");
            
            for(Group group : groupRefList) {
                long[] medianRowIds = group.getMedianRowIds();
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
                    long rowId = dataStream.readLong();
                    int timed = dataStream.readInt();
                    double xCoordinate = dataStream.readDouble();
                    double yCoordinate = dataStream.readDouble();
                    handleTuple(rowId, timed, xCoordinate, yCoordinate);
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
        
        protected void handleTuple (long rowId, int timed, double xCoordinate, double yCoordinate) {
            // ignore if it falls outside range
            if (timed < startTime || timed >= endTime || xCoordinate < minX || xCoordinate >= maxX || yCoordinate < minY || yCoordinate >= maxY) {
                return;
            }
            
            // only a single group? then it's easy, just add to that group
            if (groupCount == 1) {
                this.groupRefList[0].addTuple(rowId);
            } else {
                int indexX = (int) Math.floor(  ((double)(xCoordinate - shiftX) / stepX ) - indexMinusX );
                int indexY = (int) Math.floor(  ((double)(yCoordinate - shiftY) / stepY ) - indexMinusY );
                
                if (indexX < 0) indexX = 0;
                if (indexY < 0) indexY = 0;
                
                groupList[indexX][indexY].addTuple(rowId);
            }
        }
        
        private void determineLevel () throws SQLException {
            // find the level/part the current range fits into
            PreparedStatement q = conn.prepareStatement("SELECT level, part FROM " + tableName + "_full_index WHERE start_time <= ? AND end_time >= ? AND enabled = 1"
                    + " ORDER BY row_count ASC LIMIT 1");
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
        
        private void determineGroups () throws SQLException {
            groupRefList = new Group[groupCount];
            
            if (groupCount == 1) {
                groupRefList[0] = new Group(this, startTime, endTime, minX, maxX, minY, maxY);
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
            groupList = new Group[groupsX][groupsY];
            
            double intervalX = maxX - minX;
            double intervalY = maxY - minY;
            
            stepX = intervalX / groupsX;
            stepY = intervalY / groupsY;
            
            indexMinusX = (int) Math.floor(minX / stepX);
            indexMinusY = (int) Math.floor(minY / stepY);
            
            // shiftTime = (long)( (((double)startTime / groupDivisor ) - (double)indexMinus) * (double)groupDivisor );
            shiftX = ((minX / stepX) - indexMinusX) * stepX;
            shiftY = ((minY / stepY) - indexMinusY) * stepY;
            
            if (DEBUG) {
                    log("IntervalX: " + intervalX);
                    log("minX: " + minX);
                    log("StepX: " + stepX);
                    log("indexMinusX: " + indexMinusX);
                    log("ShiftX: " + shiftX);
                    log("----------------");
                    log("IntervalY: " + intervalY);
                    log("minY: " + minY);
                    log("StepY: " + stepY);
                    log("indexMinusY: " + indexMinusY);
                    log("ShiftY: " + shiftY);
                }
                  
            double currMinX = this.minX;
            int groupId = 0;
            for(int i=0; i < groupsX; i++) {
                double currMaxX = currMinX + stepX;
                
                double currMinY = this.minY;
                for (int j=0; j < groupsY; j++) {
                    double currMaxY = currMinY + stepY;                    
                    
                    groupList[i][j] = new Group(this, startTime, endTime, currMinX, currMaxX, currMinY, currMaxY);
                    groupRefList[groupId] = groupList[i][j];
                    groupId++;                    
                    
                    currMinY = currMaxY;
                }
                
                currMinX = currMaxX;
            }
            
        }
        
        protected double fetchValueForRowId(long rowId) throws SQLException {            
            fetchValueQuery.setLong(1, rowId);
            ResultSet res = fetchValueQuery.executeQuery();
            
            if (res.next() == false) {
                log("Unable to fetch value for rowId " + rowId);
                return -1;
            }
           
            double value = res.getDouble(columnName);           
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
