package nl.utwente.db.neogeo.preaggregate.ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.utwente.db.neogeo.preaggregate.AggrKeyDescriptor;
import nl.utwente.db.neogeo.preaggregate.PreAggregate;
import static nl.utwente.db.neogeo.preaggregate.PreAggregate.DEFAULT_KD;
import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig;
import nl.utwente.db.neogeo.preaggregate.SqlScriptBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public abstract class FinishMR extends PreAggregate {
    static final Logger logger = Logger.getLogger(FinishMR.class);
    
    protected Configuration conf;
    
    protected DbInfo dbInfo;
    
    protected FileSystem fs;
        
    protected File tempPath;
    
    protected PreAggregateConfig aggConf;
    
    protected AggrKeyDescriptor kd;

    public FinishMR (Configuration conf, DbInfo dbInfo, Connection c) {
        this.conf = conf;
        this.c = c;
        this.dbInfo = dbInfo;
        this.schema = dbInfo.getSchema();
    }
    
    public void setFS (FileSystem fs) {
        this.fs = fs;
    }
    
    public long doFinish (String jobPath, boolean deleteJob) throws IOException, FinishException, SQLException {
        long startTime = System.currentTimeMillis();
        
        // verify paths exist
        if (fs.exists(new Path(jobPath)) == false) {
            throw new FinishException("Job path does not exist on HDFS");
        }
  
        tempPath = new File(System.getProperty("java.io.tmpdir"));
        if (tempPath.exists() == false) {
            throw new IOException("Unable to find temporary directory, path '" + tempPath.getAbsolutePath() + "' does not exist");
        }
        
        // load PreAggregateConfig
        loadConfig(jobPath);
                
        // verify output directory exists
        Path outputPath = new Path(jobPath + "/output");
        if (fs.exists(outputPath) == false) {
            throw new FinishException("Output directory does not exist in job path: has the MapReduce job been run yet?");
        }
        
        try {
            kd = new AggrKeyDescriptor(aggConf.getKeyKind(), axis);
        } catch (AggrKeyDescriptor.TooManyBitsException ex) {
            throw new FinishException("Unable to initialize KeyDescriptor", ex);
        }
        
        // create PreAggregate index table
        String indexTable = createIndexTable();
        
        try {
            // insert data into index table
            insertData(jobPath, indexTable);
        } catch (Exception ex) {
            throw new FinishException("Exception during data insertion", ex);
        }
        
        if (deleteJob) {
            logger.info("Deleting job data from HDFS...");
            fs.delete(new Path(jobPath), true);
            logger.info("Job data deleted");
        }
        
        // add the index to the repository
        logger.info("Adding index to PreAggregate repository...");
        
	update_repository(c, schema, table, label, aggregateColumn, aggregateType, kd, axis, aggregateMask);
        logger.info("Index added to repository");
        
        long execTime = System.currentTimeMillis() - startTime;
        return execTime;
    }
    
    protected void prepareInsertData (String indexTable) throws IOException, FinishException {
        // can be overriden by sub-classes
    }
    
    protected void finishInsertData () throws IOException, FinishException {
        // can be overriden by sub-classes
    }
    
    // must be overriden by sub-classes
    protected abstract void insertDataIntoTable(InputStream in)  throws IOException, FinishException, SQLException;
    
    protected void insertData(String jobPath, String indexTable) throws SQLException, FileNotFoundException, IOException, FinishException, MCLParseException, MCLException {
        logger.info("Copying data into PreAggregate index table...");
        
        
        FileStatus contents[] = fs.listStatus(new Path(jobPath + "/output"));
        
        if (contents.length == 0) {
            throw new FinishException("No output files in output directory");
        }
        
        this.prepareInsertData(indexTable);
     
        String line;
        for (int i = 0; i < contents.length; i++) {
            if (!contents[i].isDir()) {
                InputStream in = fs.open(contents[i].getPath());
                insertDataIntoTable(in);
            }
        }
        
        this.finishInsertData();
        
        logger.info("Finished copying data!");
    }
    
    protected String createIndexTable () throws SQLException {
        logger.info("Creating PreAggregate index table...");
        
        SqlScriptBuilder sqlBuild = new SqlScriptBuilder(c);        
        String table = this.create_index_table(sqlBuild, null /* override_name not yet supported */, kd);
        sqlBuild.executeBatch();
        
        logger.info("Index table created");
        return table;        
    }
    
    protected void loadConfig (String jobPath) throws IOException, FinishException {
        logger.info("Loading PreAggregateConfig XML file...");
        
        Path configPath = new Path(jobPath + "/" + RunMR.CONFIG_FILENAME);
        if (fs.exists(configPath) == false) {
            throw new FinishException("Missing PreAggregateConfig XML file on HDFS in jobPath");
        }
        
        // copy config file to local filesystem
        fs.copyToLocalFile(false, configPath, new Path(tempPath.getAbsolutePath() + "/" + RunMR.CONFIG_FILENAME));
        
        File configFile = new File(tempPath.getAbsolutePath() + "/" + RunMR.CONFIG_FILENAME);
        
        if (configFile.exists() == false) {
            throw new FinishException("PreAggregateConfig XML file has not been copied to local filesystem properly!");
        }
        
        try {
            // test config file
            aggConf = new PreAggregateConfig(configFile);
        } catch (PreAggregateConfig.InvalidConfigException ex) {
            throw new IOException("Unable to read PreAggregate config file", ex);
        }
        
        // set some class fields
        this.table = aggConf.getTable();
        this.label = aggConf.getLabel();
        this.axis =  aggConf.getAxis();
        this.aggregateColumn = aggConf.getAggregateColumn();
        this.aggregateType = aggConf.getAggregateType();
        this.aggregateMask = aggConf.getAggregateMask();
        
        logger.info("Loaded config file");
    }
    
    class FinishException extends Exception {
        public FinishException (String msg) {
            super(msg);
        }
        
        public FinishException (String msg, Exception ex) {
            super(msg, ex);
        }
    }
}
