/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.neogeo.preaggregate.mapreduce;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis;
import nl.utwente.db.neogeo.preaggregate.MetricAxis;
import nl.utwente.db.neogeo.preaggregate.PreAggregate;
import nl.utwente.db.neogeo.preaggregate.SqlUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class CreateChunks extends PreAggregate {
    static final Logger logger = Logger.getLogger(CreateChunks.class);
    
    
    public CreateChunks (Connection c, String schema) {
        this.c = c;
        this.schema = schema;
    }
    
    public void create (String table, AggregateAxis axis[], String aggregateColumn, String aggregateType, int aggregateMask, int i_axisToSplit, long chunkSize, String filePath) {
        try {
            this._doCreate(table, axis, aggregateColumn, aggregateType, aggregateMask, i_axisToSplit, chunkSize, filePath);
        } catch (Exception ex) {
            logger.error("An exception occurred during creation of chunks", ex);
        }
    }
    
    public void _doCreate (String table, AggregateAxis axis[], String aggregateColumn, String aggregateType, int aggregateMask, int i_axisToSplit, long chunkSize, String filePath) throws Exception {
        logger.info("Started with chunk creation");
        long startTime = System.currentTimeMillis();
        
        int i;
        Statement q = c.createStatement();
        
        // do some sanity checks
        if (table == null || table.isEmpty()) throw new Exception("Table can't be null");
        if (axis == null || axis.length == 0) throw new Exception("Must be at least 1 axis available!");
        if (aggregateColumn == null || aggregateColumn.isEmpty()) throw new Exception("AggregateColumn can't be null or empty!");
        if (aggregateType == null || aggregateType.isEmpty()) throw new Exception("AggregateType can't be null or empty!");
        if (aggregateMask == 0) throw new Exception("AggregateMask must be bigger than zero");
        if (i_axisToSplit > (axis.length-1)) throw new Exception("AxisToSplit ID must be a valid Axis");
        
        if (filePath.endsWith("/") == false) filePath += "/";
               
        // Ensure that MonetDB has the necessary additional functions
        if (SqlUtils.dbType(c) == SqlUtils.DbType.MONETDB) {
            SqlUtils.compatMonetDb(c);
        }
        
        /*
        * First initialize and compute the aggregation axis
        */
        short maxLevel = initializeAxis(table, axis);
        
        MetricAxis axisToSplit = null;
        if ( axis[i_axisToSplit].isMetric() ) {
            axisToSplit = (MetricAxis)axis[i_axisToSplit];
        } else {
            throw new SQLException("unable to split over non-metric axis: " + i_axisToSplit);
        }
        
        // generate range functions for each axis
        logger.info("Creating range functions for each dimension...");
        for(i=0; i<axis.length; i++) {
            // generate the range conversion function for the dimension
            q.execute(axis[i].sqlRangeFunction(c, rangeFunName(i)));
        }
        logger.info("Functions created");
        
        // do actual splitting
        long nTuples = SqlUtils.count(c,schema,table,"*");
        int nChunks = (int) (nTuples / chunkSize) + 1;
        Object[][] ro = axisToSplit.split(nChunks);
        
        logger.info("Axis " + i_axisToSplit + " will be split into " + nChunks + " chunks");
                
        // create chunks
        for(i=0; i < nChunks; i++) {
            logger.info("Creating chunk #" + (i+1) + " out of " + nChunks + " chunks...");
            long startTimeChunk = System.currentTimeMillis();

            StringBuilder where = new StringBuilder();
            where.append(axisToSplit.columnExpression()).append(" >= ").append(SqlUtils.gen_Constant(c ,ro[i][0]));
            where.append(" AND ").append(axisToSplit.columnExpression()).append(" < ").append(SqlUtils.gen_Constant(c ,ro[i][1]));
            
            String chunkSelectQuery = this.select_level0(c, table, where.toString(), axis, aggregateColumn, aggregateMask);
            
            String copyQuery = SqlUtils.gen_COPY_INTO(c, chunkSelectQuery.toString(), filePath + "chunk_" + i + ".csv");
            
            logger.debug("Executing:");
            logger.debug(copyQuery);
            
            int affectedRows = q.executeUpdate(copyQuery);            
            
            long execTimeChunk = System.currentTimeMillis() - startTimeChunk;
            logger.info("Written " + affectedRows + " rows");
            logger.info("Finished chunk #" + (i+1) + " out of " + nChunks + " in " + execTimeChunk + " ms");           
        }
        
        // drop range functions
        for(i=0; i<axis.length; i++) {
            q.execute(SqlUtils.gen_DROP_FUNCTION(c, rangeFunName(i),axis[i].sqlType()));
        }
        
        q.close();
        
        long execTime = System.currentTimeMillis() - startTime;
        logger.info("Finished chunk creation in " + execTime + " ms");
    }
    
}
