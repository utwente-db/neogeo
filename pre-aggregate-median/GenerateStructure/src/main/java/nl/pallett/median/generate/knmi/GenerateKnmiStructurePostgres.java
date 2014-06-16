/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.knmi;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis
 */
public class GenerateKnmiStructurePostgres extends GenerateKnmiStructure {
    private static final Logger log = Logger.getLogger(GenerateKnmiStructurePostgres.class);

    @Override
    protected void prepareTablesFull() throws SQLException {
        Statement q = conn.createStatement();
        
        dataTable = config.getTable() + "_level" + config.getSplitFactor() + "_data_full";
        
        if (config.withValues()) {
            dataTable += "_withvalues";
        }
        
        // drop any old version of table, if exists
        q.execute("DROP TABLE IF EXISTS " + dataTable);
        
        // create new data table
        q.execute("CREATE TABLE " + dataTable + " (" +
                  "  id serial NOT NULL," +
                  "  part int NOT NULL," +
                  "  order_id int NOT NULL," +
                  "  size int NOT NULL," +
                  "  data bytea," +
                  "  CONSTRAINT " + dataTable + "_pkey PRIMARY KEY (id)" +
                  ")");
        
        q.execute("CREATE INDEX " + dataTable + "_idx" +
                  " ON " + dataTable +
                  " USING btree (part, order_id)");
        
        medianTable = config.getTable() + "_full";
        
        if (config.withValues()) {
            medianTable += "_withvalues";
        }
        
        medianTable += "_index";

        // create new median table (if only not exists yet)
        q.execute("CREATE TABLE IF NOT EXISTS " + medianTable + " (" +
                  "  part integer NOT NULL," +
                  "  level integer NOT NULL," +
                  "  start_time int," +
                  "  end_time int," +
                  "  row_count integer," +
                  "  enabled smallint DEFAULT 1," + 
                  "  CONSTRAINT " + medianTable + "_pkey PRIMARY KEY (level, part),\n" +
                  "  CONSTRAINT " + medianTable + "_part CHECK (part <= level)\n" +
                  ")");

        try {
            // test existance of index
            q.execute("SELECT '" + medianTable + "_idx'::regclass");
        } catch (SQLException e) {
            q.execute("CREATE INDEX " + medianTable + "_idx ON " + medianTable + " USING btree (start_time, end_time)");
        }

        // make sure old data for this splitFactor is removed
        q.execute("DELETE FROM " + medianTable + " WHERE level = " + config.getSplitFactor()); 

    }

    @Override
    protected void prepareTablesShifting() throws SQLException {
        Statement q = conn.createStatement();
        
        stationShiftTable = config.getTable() + "_level" + config.getSplitFactor() + "_station_shift";     
        
        if (config.datastreamsOnly() == false) {           
            // drop any old version of shift table, if exists
            q.execute("DROP TABLE IF EXISTS " + stationShiftTable);

            // create new shift table
            q.execute("CREATE TABLE " + stationShiftTable + " (" +
                      "  station int NOT NULL," +
                      "  part int NOT NULL," +
                      "  shift_left int," +
                      "  shift_right int," +
                      "  CONSTRAINT " + stationShiftTable + "_pkey PRIMARY KEY (part, station)" +
                      ")");
        }
        
        timeShiftTable = config.getTable() + "_level" + config.getSplitFactor() + "_time_shift";
        
        if (config.datastreamsOnly() == false) {        

            // drop any old version of shift table, if exists
            q.execute("DROP TABLE IF EXISTS " + timeShiftTable);

            // create new shift table
            q.execute("CREATE TABLE " + timeShiftTable + " (" +
                      "  station int NOT NULL," +
                      "  timed int NOT NULL," +
                      "  part int NOT NULL," +
                      "  shift_left int," +
                      "  shift_right int," +
                      "  CONSTRAINT " + timeShiftTable + "_pkey PRIMARY KEY (station, timed, part)" +
                      ")");

            q.execute("CREATE INDEX " + timeShiftTable + "_idx" +
                      " ON " + timeShiftTable + " " +
                      "  USING btree (station, timed)");     
        }
        
        dataTable = config.getTable() + "_level" + config.getSplitFactor() + "_data";
        
        if (config.withValues()) {
            dataTable += "_withvalues";
        }
        
        // drop any old version of table, if exists
        q.execute("DROP TABLE IF EXISTS " + dataTable);
        
        // create new data table
        q.execute("CREATE TABLE " + dataTable + " (" +
                  "  id serial NOT NULL," +
                  "  part integer," +
                  "  direction integer," +
                  "  size bigint," +
                  "  data bytea," +
                  "  CONSTRAINT " + dataTable + "_pkey PRIMARY KEY (id)," +
                  "  CONSTRAINT valid_direction CHECK (direction = (-1) OR direction = 1)" +
                  ")");
        
        medianTable = config.getTable();
        
        if (config.withValues()) {
            medianTable += "_withvalues";
        }
        
        medianTable +=  "_median";
                
        // create new median table (if only not exists yet)
        q.execute("CREATE TABLE IF NOT EXISTS " + medianTable + " (" +
                  "  part integer NOT NULL," +
                  "  level integer NOT NULL," +
                  "  start_time int," +
                  "  end_time int," +
                  "  median_rowid_1 int NOT NULL," +
                  "  median_time_1 int NOT NULL, " +
                  "  median_station_1 int NOT NULL, " +
                  "  median_temperature_1 smallint, " +
                  "  median_rowid_2 int," +
                  "  median_time_2 int," +
                  "  median_station_2 int," +
                  "  median_temperature_2 smallint, " +
                  "  row_count integer," +
                  "  enabled smallint DEFAULT 1," + 
                  "  CONSTRAINT " + medianTable + "_pkey PRIMARY KEY (level, part),\n" +
                  "  CONSTRAINT " + medianTable + "_part CHECK (part <= level)\n" +
                  ")");

        // make sure old data for this splitFactor is removed
        q.execute("DELETE FROM " + medianTable + " WHERE level = " + config.getSplitFactor()); 
    }

    @Override
    protected void finalizeTables() throws SQLException {
        log.info("Finalizing tables...");
        
        Statement q = conn.createStatement();
        
        if (timeShiftTable != null && config.datastreamsOnly() == false) {
            q.execute("VACUUM ANALYZE " + timeShiftTable);
        }
        
        if (stationShiftTable != null && config.datastreamsOnly() == false) {
            q.execute("VACUUM ANALYZE " + stationShiftTable);
        }
        
        q.execute("VACUUM ANALYZE " + dataTable);
        q.execute("VACUUM ANALYZE " + medianTable);
        
        log.info("Tables finalized");
    }

    @Override
    protected int[] getMinMaxTime() throws SQLException {
        // get min time and max time from table
        log.info("Fetching MIN and MAX timed from table...");
        PreparedStatement q = conn.prepareStatement("SELECT MIN(timed) AS min_time, MAX(timed) AS max_time FROM " + config.getTable());
        ResultSet res = q.executeQuery();
        
        if (res.next() == false) {
            throw new SQLException("Unable to determine MIN/MAX timed for table");
        }
        
        int minTime = res.getInt("min_time");
        int maxTime = res.getInt("max_time");
        
        return new int[]{minTime, maxTime};
    }

    @Override
    protected ProcessKnmiPart createProcessPartObject() {
        return new ProcessKnmiPartPostgres();
    }
    
}
