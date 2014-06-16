/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate.twitter;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

/**
 *
 * @author
 * Dennis
 */
public class GenerateTwitterStructurePostgres extends GenerateTwitterStructure {
    private static final Logger log = Logger.getLogger(GenerateTwitterStructurePostgres.class);
    
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
    protected void finalizeTables() throws SQLException {
        log.info("Finalizing tables...");
        
        Statement q = conn.createStatement();
        
        if (shiftTable != null && config.datastreamsOnly() == false) {
            q.execute("VACUUM ANALYZE " + shiftTable);
        }
        
        q.execute("VACUUM ANALYZE " + dataTable);
        q.execute("VACUUM ANALYZE " + medianTable);
        
        log.info("Tables finalized");
    }
    
    @Override
    protected ProcessTwitterPart createProcessPartObject() {
        return new ProcessTwitterPartPostgres();
    }

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
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
