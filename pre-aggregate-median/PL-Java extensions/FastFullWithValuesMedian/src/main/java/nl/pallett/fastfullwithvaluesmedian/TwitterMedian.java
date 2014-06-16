package nl.pallett.fastfullwithvaluesmedian;

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

        provider.calculateMedians();
        
        // output final execution time
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
        
        protected double median;
        
        protected ArrayList<Short> valueList;
                
        public Group (int startTime, int endTime, double minX, double maxX, double minY, double maxY) {
            this.startTime = startTime;
            this.endTime = endTime;
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
            
            this.valueList = new ArrayList<Short>();
        }
               
        public int getRowCount () {
            return this.valueList.size();
        }
        
        public void setMedian (double median) {
            this.median = median;
        }
        
        public double getMedian () {
            return this.median;
        }
        
        public short[] getMedianValues() {
            short[] ret = {};
            
            if (valueList.size() > 0) {            
                if (valueList.size() % 2 == 0) {
                    int index1 = (int) (valueList.size() / 2) - 1;
                    int index2 = index1 + 1;

                    ret = new short[]{valueList.get(index1), valueList.get(index2)};
                } else {
                    int index = (int) Math.floor(valueList.size() / 2);
                    ret = new short[]{valueList.get(index)};
                }
            }
            
            return ret;
        }

        public void addTuple (short value) {            
            this.valueList.add(value);
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
                
        public void prepare () throws SQLException {           
            determineGroups();
            determineLevel();
        }
        
        public void calculateMedians () throws SQLException {
            scanData();
            determineMedians();
        }
                
        private void determineMedians () throws SQLException {            
            for(Group group : groupRefList) {
                short[] medianValues = group.getMedianValues();
                double median = 0;
                
                if (medianValues.length == 2) {
                    median = (((double)(medianValues[0]) + medianValues[1])) / 2;
                } else if (medianValues.length == 1) {
                    median = (double)medianValues[0];
                }
                
                group.setMedian(median);
            }
        }
        
        
        private boolean scanData () throws SQLException {
            Statement q = conn.createStatement();
            buffer = q.executeQuery("SELECT size, data FROM " + tableName + "_level" + level + "_data_full_withvalues WHERE part = " + this.part + " ORDER BY id ASC");
            
            while(true) {
                if (dataStream == null || dataStreamSize == dataStreamPos) {
                    if (getNextBuffer() == false) {
                        break;
                    }
                }
                
                try {
                    int timed = dataStream.readInt();
                    double xCoordinate = dataStream.readDouble();
                    double yCoordinate = dataStream.readDouble();
                    short value = dataStream.readShort();
                    handleTuple(timed, xCoordinate, yCoordinate, value);
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
        
        protected void handleTuple (int timed, double xCoordinate, double yCoordinate, short value) {
            // ignore if it falls outside range
            if (timed < startTime || timed >= endTime || xCoordinate < minX || xCoordinate >= maxX || yCoordinate < minY || yCoordinate >= maxY) {
                return;
            }
            
            // only a single group? then it's easy, just add to that group
            if (groupCount == 1) {
                this.groupRefList[0].addTuple(value);
            } else {
                int indexX = (int) Math.floor(  ((double)(xCoordinate - shiftX) / stepX ) - indexMinusX );
                int indexY = (int) Math.floor(  ((double)(yCoordinate - shiftY) / stepY ) - indexMinusY );
                
                if (indexX < 0) indexX = 0;
                if (indexY < 0) indexY = 0;
                
                groupList[indexX][indexY].addTuple(value);
            }
        }
        
        private void determineLevel () throws SQLException {
            // find the level/part the current range fits into
            PreparedStatement q = conn.prepareStatement("SELECT level, part FROM " + tableName + "_full_withvalues_index WHERE start_time <= ? AND end_time >= ? AND enabled = 1"
                    + " ORDER BY row_count ASC LIMIT 1");
            q.setLong(1, startTime);
            q.setLong(2, endTime);
            
            ResultSet res = q.executeQuery();
            if (res.next() == false) {
                res.close();
                q.close();
                
                q = conn.prepareStatement("SELECT * FROM " + tableName + "_full_withvalues_index WHERE level = 1 AND part = 1");
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
                groupRefList[0] = new Group(startTime, endTime, minX, maxX, minY, maxY);
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
                    
                    groupList[i][j] = new Group(startTime, endTime, currMinX, currMaxX, currMinY, currMaxY);
                    groupRefList[groupId] = groupList[i][j];
                    groupId++;                    
                    
                    currMinY = currMaxY;
                }
                
                currMinX = currMaxX;
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
