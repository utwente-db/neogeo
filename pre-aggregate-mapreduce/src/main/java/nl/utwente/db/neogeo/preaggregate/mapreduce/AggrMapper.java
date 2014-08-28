package nl.utwente.db.neogeo.preaggregate.mapreduce;

import au.com.bytecode.opencsv.CSVReader;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import nl.utwente.db.neogeo.preaggregate.AggrKey;
import nl.utwente.db.neogeo.preaggregate.AggrKeyDescriptor;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis;
import static nl.utwente.db.neogeo.preaggregate.PreAggregate.AGGR_COUNT;
import static nl.utwente.db.neogeo.preaggregate.PreAggregate.AGGR_MAX;
import static nl.utwente.db.neogeo.preaggregate.PreAggregate.AGGR_MIN;
import static nl.utwente.db.neogeo.preaggregate.PreAggregate.AGGR_SUM;
import static nl.utwente.db.neogeo.preaggregate.PreAggregate.DEFAULT_KD;
import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig;
import nl.utwente.db.neogeo.preaggregate.ui.RunMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class AggrMapper<KEYOUT extends Object, VALUEOUT extends Object> extends Mapper<NullWritable, Text, KEYOUT, VALUEOUT> {
    public static final int BATCH_SIZE = 200;
    
    static final Logger logger = Logger.getLogger(AggrMapper.class);
        
    protected Connection conn;
    
    protected AggregateAxis[] axis;
    
    protected int aggregateMask;
    
    protected AGGR_TYPE aggregateType;
    
    protected AggrKeyDescriptor kd;
    
    protected Configuration conf;
    
    protected PreAggregateConfig aggConf;
    
    protected AggrKey aggrKey;
    
    public void setConfiguration (Configuration conf) {
        this.conf = conf;
    }
    
    @Override
    public void setup (Context context) throws IOException {
        if (conf == null) {
            conf = context.getConfiguration();
        }
        
        loadConfig(context);
        
        aggregateMask = aggConf.getAggregateMask();
        if (aggregateMask == -1) {
            throw new IllegalArgumentException("Invalid aggregate mask specified");
        }
            
        initializeAxis(context);
        
        try {
            setupConn();
            importLfpTable();
            createDataTable();            
        } catch (IOException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
    
    protected void loadConfig(Context context) throws IOException {
        File f = new File("./" + RunMR.CONFIG_FILENAME);
        
        if (f.exists() == false) {
            throw new IOException("PreAggregate config file missing in DistributedCache!");
        }
        
        try {
            aggConf = new PreAggregateConfig(f);
        } catch (PreAggregateConfig.InvalidConfigException ex) {
            throw new IOException("Unable to load PreAggregate config file", ex);
        }
        
        if (aggConf.getAggregateType().equalsIgnoreCase("int") || aggConf.getAggregateType().equalsIgnoreCase("integer")) {
            aggregateType = AGGR_TYPE.TYPE_INT;
        } else if (aggConf.getAggregateType().equalsIgnoreCase("bigint")) {
            aggregateType = AGGR_TYPE.TYPE_BIGINT;
        } else if (aggConf.getAggregateType().equalsIgnoreCase("double") || aggConf.getAggregateType().equalsIgnoreCase("double precision")) {
            aggregateType = AGGR_TYPE.TYPE_DOUBLE;
        } else {
            throw new UnsupportedOperationException("AggregateType of '" + aggConf.getAggregateType() + "' not yet supported!");
        }
    }
    
    protected void initializeAxis (Context context) throws IOException {         
        // initialize axis array
        this.axis = aggConf.getAxis();
        
        try {
            // initialize KeyDescriptor
            kd = new AggrKeyDescriptor(aggConf.getKeyKind(), axis);
        } catch (AggrKeyDescriptor.TooManyBitsException ex) {
            throw new IOException("Unable to initialize KeyDescriptor", ex);
        }
        
        aggrKey = new AggrKey(kd);
    }
    
    protected void createDataTable() throws SQLException {
        Statement q = conn.createStatement ();
        
        StringBuilder create = new StringBuilder("CREATE TABLE data (");
        for(int i=0; i < axis.length; i++) {
            create.append("l").append(i).append(" int, ");
            create.append("i").append(i).append(" int, ");
        }
        create.append("countaggr int, ");
        create.append("sumaggr ").append(aggregateType.sqlName()).append(", ");
        create.append("minaggr ").append(aggregateType.sqlName()).append(", ");
        create.append("maxaggr ").append(aggregateType.sqlName());
        create.append(");");
        
        q.execute(create.toString());
    }
    
    protected void importLfpTable () throws SQLException, FileNotFoundException, IOException {
        long start = System.currentTimeMillis();
        logger.debug("Importing level/factor possibilities table...");
                
        Statement q = conn.createStatement();
        
        // build queries to create and insert into helper table
        StringBuilder createTable = new StringBuilder("CREATE TABLE _ipfx_lfp (");
        StringBuilder insertQuery = new StringBuilder("INSERT INTO _ipfx_lfp VALUES (");
        
        for(int i=0; i < axis.length; i++) {
            createTable.append("target_l").append(i).append(" INTEGER, \n");
            createTable.append("source_l").append(i).append(" INTEGER, \n");
            createTable.append("factor_f").append(i).append(" BIGINT");
            
            insertQuery.append("?, ?, ?");
            
            if (i+1 < axis.length) {
                createTable.append(", \n");
                insertQuery.append(", ");
            }
        }
        createTable.append(");");
        insertQuery.append(");\n");

        // execute create table query
        q.execute(createTable.toString());
        q.close();
        
        PreparedStatement insert = conn.prepareStatement(insertQuery.toString());
        
        File f = new File("./" + RunMR.LFP_TABLE_FILENAME);
        
        if (f.exists() == false) {
            throw new IOException("LFP table CSV file missing in DistributedCache!");
        }
                
        CSVReader reader = new CSVReader(new FileReader(f));
        
        String [] row;
        while ((row = reader.readNext()) != null) {
            // skip near empty row without any fuss
            if (row.length < 2) continue;
            
            // verify row has correct number of fields
            // each axis has 3 fields
            if (row.length != (axis.length*3)) {
                logger.warn("Skipping _ipfx_lfp row, not enough fields!");
                continue;
            }
                    
            for(int i=0; i < row.length; i++) {
                int val = Integer.parseInt(row[i]);                
                insert.setInt(i+1, val);
            }
                        
            insert.addBatch();
        }
        reader.close();
        
        insert.executeBatch();
        
        int updateCount = insert.getUpdateCount();
        insert.close();
        
        long time = System.currentTimeMillis() - start;
        logger.debug("Finished importing LFP table in " + time + " ms");
    }
    
    protected void setupConn () throws SQLException {
        long start = System.currentTimeMillis();
        logger.debug("Creating in-memory DB...");
        
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException ex) {
            throw new SQLException(ex);
        }

        conn = DriverManager.getConnection("jdbc:h2:mem:;LOG=0;CACHE_SIZE=65536;LOCK_MODE=0;UNDO_LOG=0");
        
        long time = System.currentTimeMillis() - start;
        logger.debug("DB created in " + time + " ms");
    }
    
    @Override
    public void map (NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        logger.debug("Started map task...");
        
        try {
            // insert CSV value into database
            insertCsv(value);
            
            // generate additional levels
            generateLevels();
            
            // emit data
            outputData(context);
        } catch (SQLException ex) {
            throw new IOException(ex);
        }   
        
        // clean up data table
        try {
            Statement q = conn.createStatement();
            q.execute("DELETE FROM data");
            q.close();
        } catch (SQLException ex) {
            throw new IOException("Unable to clear data table");
        }
        
            
        long execTime = System.currentTimeMillis() - startTime;
        logger.debug("Finished map task in " + execTime + " ms");
    }
    
    protected void generateLevels () throws SQLException {
        // first compute the total number of levels
        int sumLevels = 0;
        for(int i=0; i < axis.length; i++) {
            sumLevels += axis[i].maxLevels();
        }
        
        Statement q = conn.createStatement();
        for(int thisLevel=1; thisLevel <= sumLevels; thisLevel++) {
            StringBuilder select = new StringBuilder();
            StringBuilder where = new StringBuilder();
            StringBuilder gb = new StringBuilder();

            select.append("SELECT");
            for(int i=0; i <axis.length; i++) {
                select.append("\ttarget_l").append(i).append(" AS ").append("l").append(i).append(", ");
                select.append("level0.i").append(i).append(" / factor_f").append(i).append(" as ii").append(i);

                if ( i != axis.length-1 ) select.append(",\n");
            }
            
            if ((aggregateMask & AGGR_COUNT) != 0)
                    select.append(",\n\tSUM(level0.countAggr) AS countAggr");
            if ((aggregateMask & AGGR_SUM) != 0)
                    select.append(",\n\tSUM(level0.sumAggr) AS sumAggr");
            if ((aggregateMask & AGGR_MIN) != 0)
                    select.append(",\n\tMIN(level0.minAggr) AS minAggr");
            if ((aggregateMask & AGGR_MAX) != 0)
                    select.append(",\n\tMAX(level0.maxAggr) AS maxAggr");
            
            where.append("WHERE");
            StringBuilder lsum = new StringBuilder();
            for(int i=0; i <axis.length; i++) {
                where.append("\tlevel0.l").append(i).append("=source_l").append(i).append(" and\n");
                
                if ( i>0 ) lsum.append("+");
                lsum.append("target_l").append(i);
            }
            where.append("\t(").append(lsum).append(")=").append(thisLevel).append("\n");
            
            gb.append("GROUP BY");
            for(int i=0; i<axis.length; i++) {
                if ( i >0 ) gb.append(",");
                gb.append(" target_l").append(i).append(", ii").append(i).append(", factor_f").append(i);
            }

            String stat = "INSERT INTO data\n"+
               select +
               "\nFROM _ipfx_lfp, data AS level0\n" +
               where +
               gb + ";";
            
            logger.debug("Executing query:");
            logger.debug(stat);
            
            long start = System.currentTimeMillis();
            q.execute(stat);
            long exec = System.currentTimeMillis() - start;
            
            logger.debug("Inserted " + q.getUpdateCount() + " rows in " + exec + " ms");
        }
        
        q.close();
    }
        
    //protected abstract void emit (Context context, KEYOUT ckey, ResultSet res) throws IOException, SQLException, InterruptedException;
    
    protected void outputData(Context context) throws SQLException, IOException, InterruptedException {
        Statement q = conn.createStatement();
        
        ResultSet res = q.executeQuery("SELECT * from data");        
        while(res.next()) {            
            KEYOUT key = (KEYOUT)computeAggrKey(res);         
                        
            if (context != null) {                
                VALUEOUT value = null;
                if (this.aggregateType == AGGR_TYPE.TYPE_INT) {
                    value = (VALUEOUT)(new IntAggrWritable(res.getInt("countaggr"), res.getInt("sumaggr"), res.getInt("minaggr"), res.getInt("maxaggr")));
                } else if (this.aggregateType == AGGR_TYPE.TYPE_BIGINT) {
                    value = (VALUEOUT)(new LongAggrWritable(res.getLong("countaggr"), res.getLong("sumaggr"), res.getLong("minaggr"), res.getLong("maxaggr")));
                } else if (this.aggregateType == AGGR_TYPE.TYPE_DOUBLE) {
                    value = (VALUEOUT)(new DoubleAggrWritable(res.getLong("countaggr"), res.getDouble("sumaggr"), res.getDouble("minaggr"), res.getDouble("maxaggr")));
                }
                
                if (key != null && value != null) {
                    context.write(key, value);
                }
            } else {            
                logger.debug(key + "\t" + res.getInt("countaggr") + "\t" + res.getInt("sumaggr"));
            }
        }
        
        res.close();
        q.close();
    }
    
    protected Object computeAggrKey (ResultSet rs) throws SQLException {
        for(short i=0; i < axis.length; i++) { 
            aggrKey.setIndex(i, rs.getShort("i" + i));
            aggrKey.setLevel(i, rs.getShort("l" + i));
        }
        
        Object genKey = aggrKey.toKey();
        Object ret = null;
        
        if (genKey instanceof Long) {
            ret = new LongWritable(((Long)genKey).longValue());
        } else if (genKey instanceof String) {
            ret = new Text(genKey.toString());
        }
        
        return ret;
    }
    
    protected void insertCsv (Text value) throws SQLException, IOException {
        // build insert query
        StringBuilder insertQuery = new StringBuilder("INSERT INTO data VALUES (");
        for(int i=0; i < axis.length; i++) {
            insertQuery.append("?, ?, ");
        }
        insertQuery.append("?, ?, ?, ?)");
        
        // convert value into Reader
        byte[] arrBytes = value.getBytes();
        BufferedReader in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(arrBytes)));
        
        // read CSV into database        
        CSVReader reader = new CSVReader(in);               
        PreparedStatement insert = conn.prepareStatement(insertQuery.toString());
        String [] row;
        int counter = 0;
        int rowLength = (axis.length * 2) + 4; // expected length of rows
        int countIdx = rowLength - 4 + 1;
        int sumIdx = countIdx + 1;
        int minIdx = sumIdx + 1;
        int maxIdx = minIdx + 1;        
        while ((row = reader.readNext()) != null) {
            if (row.length != rowLength) continue;
            
            // insert axis values
            int i;
            for(i=0; i < axis.length; i++) {
                int val1 = Integer.parseInt(row[i*2]);
                int val2 = Integer.parseInt(row[(i*2)+1]);
                insert.setInt(i*2 + 1, val1);
                insert.setInt(i*2 + 2, val2);
            }

            // insert aggregate values
            insert.setLong(countIdx, Integer.parseInt(row[countIdx-1]));
            
            if (aggregateType == AGGR_TYPE.TYPE_BIGINT) {
                i++;
                insert.setLong(sumIdx, Long.parseLong(row[sumIdx-1]));
                i++;
                insert.setLong(minIdx, Long.parseLong(row[minIdx-1]));
                i++;
                insert.setLong(maxIdx, Long.parseLong(row[maxIdx-1]));
            } else if (aggregateType == AGGR_TYPE.TYPE_INT) {
                i++;
                insert.setInt(sumIdx, Integer.parseInt(row[sumIdx-1]));
                i++;
                insert.setInt(minIdx, Integer.parseInt(row[minIdx-1]));
                i++;
                insert.setInt(maxIdx, Integer.parseInt(row[maxIdx-1]));
            } else if (aggregateType == AGGR_TYPE.TYPE_DOUBLE) {
                i++;
                insert.setDouble(sumIdx, Double.parseDouble(row[sumIdx-1]));
                i++;
                insert.setDouble(minIdx, Double.parseDouble(row[minIdx-1]));
                i++;
                insert.setDouble(maxIdx, Double.parseDouble(row[maxIdx-1]));
            } else {
                throw new UnsupportedOperationException("AggregateType " + aggregateType + " not yet supported");
            }

            insert.addBatch();
            counter++;

            if (counter % BATCH_SIZE == 0) {
                insert.executeBatch();
            }
        } 

        // insert final batch
        if (counter % BATCH_SIZE != 0) {
            insert.executeBatch();
        }
        
        insert.close();

        logger.debug("Inserted " + counter + " rows into data table");
        reader.close();
    }
    
    @Override
    public void cleanup(Context context) {
        try {
            conn.close();
        } catch (SQLException ex) {
            logger.warn("Unable to close connection");
        }
    }
    
}
