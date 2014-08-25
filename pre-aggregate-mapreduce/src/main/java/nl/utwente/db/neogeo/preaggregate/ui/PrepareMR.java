package nl.utwente.db.neogeo.preaggregate.ui;

import au.com.bytecode.opencsv.CSVWriter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.logging.Level;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis;
import nl.utwente.db.neogeo.preaggregate.MetricAxis;
import nl.utwente.db.neogeo.preaggregate.PreAggregate;
import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig;
import nl.utwente.db.neogeo.preaggregate.SqlScriptBuilder;
import nl.utwente.db.neogeo.preaggregate.SqlUtils;
import nl.utwente.db.neogeo.preaggregate.SqlUtils.DbType;
import static nl.utwente.db.neogeo.preaggregate.ui.CreateChunks.logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class PrepareMR extends PreAggregate {
    static final Logger logger = Logger.getLogger(PrepareMR.class);
    
    protected Configuration conf;
    
    protected DbInfo dbInfo;
    
    protected FileSystem fs;
    
    protected BufferedMCLReader mapiIn;
    protected BufferedMCLWriter mapiOut;
    
    protected File tempPath;
    
    protected String jobPath;
    
    public PrepareMR (Configuration conf, DbInfo dbInfo, Connection c, String table, String override_name, String label, 
                      AggregateAxis axis[], String aggregateColumn, String aggregateType, int aggregateMask)  {
        this.conf = conf;
        this.c = c;
        this.dbInfo = dbInfo;
        this.schema = dbInfo.getSchema();
        this.table = table;
        this.label = label;
        this.axis = axis;
        this.aggregateColumn = aggregateColumn;
        this.aggregateType = aggregateType;
        this.aggregateMask = aggregateMask;
    }
    
    public PrepareMR (Configuration conf, DbInfo dbInfo, Connection c, PreAggregateConfig config) {
        this.conf = conf;
        this.c = c;
        this.dbInfo = dbInfo;
        this.schema = dbInfo.getSchema();
        this.table = config.getTable();
        this.label = config.getLabel();
        this.axis =  config.getAxis();
        this.aggregateColumn = config.getAggregateColumn();
        this.aggregateType = config.getAggregateType();
        this.aggregateMask = config.getAggregateMask();
    }
    
    public void setFS (FileSystem fs) {
        this.fs = fs;
    }
    
    public void doPrepare (String jobPath, int axisToSplitIdx, long chunkSize) throws PrepareException, SQLException, IOException {       
        Statement q = c.createStatement();
        
        this.jobPath = jobPath;
        
        // sanity checks
        if (axisToSplitIdx >= axis.length) throw new PrepareException("Axis to split index is invalid");
        if (chunkSize == 0) throw new PrepareException("Chunk size cannot be zero!");
        
        // Ensure that MonetDB has the necessary additional functions
        if (SqlUtils.dbType(c) == SqlUtils.DbType.MONETDB) {
            SqlUtils.compatMonetDb(c);
        }
        
        tempPath = new File(System.getProperty("java.io.tmpdir"));
        if (tempPath.exists() == false) {
            throw new IOException("Unable to find temporary directory, path '" + tempPath.getAbsolutePath() + "' does not exist");
        }
        
        logger.debug("Temp directory: " + tempPath.getAbsolutePath());
        
        // initialise HDFS
        if (fs == null) fs = FileSystem.get(conf);
        
        // check if jobPath already exists on HDFS
        if (fs.exists(new Path(jobPath))) {
            logger.warn("Job path already exists on HDFS. Delete existing directory? (yes|no)");
            Scanner scan = new Scanner(System.in);
            String s = scan.next().toLowerCase();
            
            while(s.startsWith("y") == false && s.startsWith("n") == false) {
                logger.warn("Delete existing directory? (yes|no)");
                s = scan.next().toLowerCase();
            }
            
            if (s.toLowerCase().startsWith("n")) {
                logger.error("Quiting!");
                System.exit(0);
            }
            
            // delete directory
            logger.info("Deleting existing job path...");
            fs.delete(new Path(jobPath), true);
            logger.info("Path deleted");
        }
        
        short maxLevel = initializeAxis(table, axis);
        
        MetricAxis axisToSplit = null;
        if ( axis[axisToSplitIdx].isMetric() ) {
            axisToSplit = (MetricAxis)axis[axisToSplitIdx];
        } else {
            throw new SQLException("unable to split over non-metric axis: " + axisToSplitIdx);
        }
        
        String dimTable[] = new String[axis.length];
        SqlScriptBuilder sqlBuildDimTables = new SqlScriptBuilder(c);
        
        // generate range functions for each axis
        logger.info("Creating range functions for each dimension...");
        for(int i=0; i<axis.length; i++) {
            // generate the range conversion function for the dimension
            q.execute(axis[i].sqlRangeFunction(c, rangeFunName(i)));
            
            // generate the dimension level/factor value table
            dimTable[i] = create_dimTable(c, schema,i,axis[i].N(),axis[i].maxLevels(), sqlBuildDimTables);
        }
        logger.info("Functions created");
        
        logger.info("Creating dimension level/factor tables...");
        sqlBuildDimTables.executePreBatch();
        logger.info("Tables created");
        
        // create level/factor possibilities helper table
        createLfpTable(dimTable);
                
        // do actual splitting
        long nTuples = SqlUtils.count(c,schema,table,"*");
        int nChunks = (int) (nTuples / chunkSize) + 1;
        Object[][] ro = axisToSplit.split(nChunks);
        
        logger.info("Axis " + axisToSplitIdx + " (" + axisToSplit.columnExpression() + ") will be split into " + nChunks + " chunks");
        
        try {
            // create and upload chunks
            createChunks(nChunks, axisToSplit, ro, jobPath);
        } catch (MCLParseException ex) {
            throw new PrepareException("Error occured during raw MAPI connection with MonetDB database", ex);
        } catch (MCLException ex) {
            throw new PrepareException("Error occured during raw MAPI connection with MonetDB database", ex);
        }
        
        // drop range functions
        logger.info("Dropping range functions for each dimension...");
        for(int i=0; i<axis.length; i++) {
            q.execute(SqlUtils.gen_DROP_FUNCTION(c, rangeFunName(i),axis[i].sqlType()));
        }
        logger.info("Functions dropped");
        
        logger.info("Dropping dimension level/factor tables...");
        sqlBuildDimTables.executePostBatch();
        logger.info("Tables dropped");
        
        logger.info("Writing PreAggregate config to HDFS...");
        createConfig();
        logger.info("Config has been written");
    }
    
    protected void createConfig () throws PrepareException, IOException {
        logger.info("Creating and uploading PreAggregate config file...");
        
        PreAggregateConfig config = new PreAggregateConfig(table, this.aggregateColumn, label, aggregateType, aggregateMask, axis);
        
        String fileName = "preaggregate.xml";
        try {
            config.writeToXml(new File(tempPath + "/" + fileName));
        } catch (Exception ex) {
            throw new PrepareException("Unable to write PreAggregateConfig XML file to temp directory", ex);
        }
        
        fs.copyFromLocalFile(false, true, new Path(tempPath + "/" + fileName), new Path(jobPath + "/" + fileName));
        
        logger.info("Uploaded PreAggregate config file!");
    }
    
    protected void createLfpTable (String[] dimTable) throws SQLException, IOException {
        logger.info("Creating level/factor possibilities helper table...");
        
        String lfp_table = schema + "._ipfx_lfp";
        SqlScriptBuilder sql_build = new SqlScriptBuilder(c);
        gen_lfp_table(c, sql_build , lfp_table, axis, dimTable);
        
        sql_build.executePreBatch();
        
        logger.info("Table created");
        
        logger.info("Exporting to CSV...");
               
        String fileName = "lfp_table.csv";
        CSVWriter writer = new CSVWriter(new FileWriter(tempPath.getAbsolutePath() + "/" + fileName), ',');
        
        Statement q = c.createStatement();
        ResultSet res = q.executeQuery("SELECT * FROM " + lfp_table);
        
        // dump to CSV
        writer.writeAll(res, false);
        
        writer.close();
        res.close();
        q.close();
        
        // send to HDFS
        logger.info("CSV created, uploading to HDFS...");
        fs.copyFromLocalFile(false, true, new Path(tempPath + "/" + fileName), new Path(jobPath + "/" + fileName));
        logger.info("Upload succesfully finished");    
        
        sql_build.executePostBatch();
        logger.info("Finished level/factor possibilities helper table!");
    }
    
    protected void createChunks (int nChunks, MetricAxis axisToSplit, Object[][] ro, String jobPath) throws IOException, MCLParseException, MCLException, PrepareException, SQLException {
        logger.info("Creating chunks...");
        
        MapiSocket server = new MapiSocket();

        server.setDatabase(dbInfo.getDatabase());
        server.setLanguage("sql");
        
        List warning = server.connect(dbInfo.getHostname(), dbInfo.getPort(), dbInfo.getUsername(), dbInfo.getPassword());
        if (warning != null) {
            for (Iterator it = warning.iterator(); it.hasNext();) {
                logger.warn(it.next().toString());
            }
        }
        
        mapiIn = server.getReader();
        mapiOut = server.getWriter();

        String error = mapiIn.waitForPrompt();
        if (error != null) {
            throw new PrepareException(error);
        }
        
        // start upload thread
        UploadThread upload = new UploadThread(fs, tempPath, jobPath);
        upload.start();
        
        for(int i=0; i < nChunks; i++) {
            logger.info("Creating chunk #" + (i+1) + " out of " + nChunks + " chunks...");
            long startTimeChunk = System.currentTimeMillis();
            
            // open new file for this chunk
            String fileName = "/chunk_" + i + ".csv";
            Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempPath.getAbsolutePath() + fileName), "utf-8"));
            
            StringBuilder where = new StringBuilder();
            where.append(axisToSplit.columnExpression()).append(" >= ").append(SqlUtils.gen_Constant(c ,ro[i][0]));
            where.append(" AND ").append(axisToSplit.columnExpression()).append(" < ").append(SqlUtils.gen_Constant(c ,ro[i][1]));
            
            int affectedRows = writeChunk(writer, where);
                        
            // finalize chunk
            writer.close();
            
            // add to upload queue
            upload.addToQueue(fileName);
            
            long execTimeChunk = System.currentTimeMillis() - startTimeChunk;
            logger.info("Written " + affectedRows + " rows");
            logger.info("Finished chunk #" + (i+1) + " out of " + nChunks + " in " + execTimeChunk + " ms");
            
            if (upload.hasExceptions()) {
                throw new PrepareException("Uploading failed", upload.lastException());
            }
        }
        
        server.close();
        
        // tell upload thread no more chunks are coming
        upload.setCreationFinished();
                
        logger.info("Finished creating chunks!");
        logger.info("Waiting for all chunks to be uploaded...");
        
        if (upload.hasExceptions()) {
            throw new PrepareException("Uploading failed", upload.lastException());
        }
        
        // wait until uploading is finished
        while(upload.isFinished() == false) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                // don't care
            }
        }
        
        logger.info("All chunks created and uploaded to HDFS!");
    }
    
    protected int writeChunk(Writer writer, StringBuilder where) throws IOException, SQLException {
        String chunkSelectQuery = this.select_level0(c, table, where.toString(), axis, aggregateColumn, aggregateMask);
        
        int ret = 0;
        if (SqlUtils.dbType(c) == DbType.MONETDB) {
            // the leading 's' is essential, since it is a protocol
            // marker that should not be omitted, likewise the
            // trailing semicolon
            mapiOut.write('s');
            
            String copyQuery = "COPY " + chunkSelectQuery + " INTO STDOUT USING DELIMITERS ',','\\n';";
            
            mapiOut.write(copyQuery);
            mapiOut.newLine();
            mapiOut.writeLine("");
            
            String line;
            while((line = mapiIn.readLine()) != null) {
                int lineType = mapiIn.getLineType();

                // when PROMPT is reached all data has been read
                if (lineType == BufferedMCLReader.PROMPT) break;

                // ignore all other official lines
                if (lineType != 0) continue;

               writer.write(line);
               writer.write("\n");
               ret++;
            }
        } else {
            throw new UnsupportedOperationException("Database type " + SqlUtils.dbType(c) + " not yet supported");
        }
        
        return ret;
    }
    
    class UploadThread extends Thread {
        protected FileSystem fs;
        
        protected String jobPath;
        
        protected File tempPath;
        
        protected boolean isFinished = false;
        
        protected boolean isCreationFinished = false;
        
        protected Queue<String> uploadQueue = new LinkedList<String>();
        
        protected boolean hasExceptions = false;
        
        protected Exception lastException = null;
        
        public UploadThread (FileSystem fs, File tempPath, String jobPath) {
            super();
            
            this.fs = fs;
            this.tempPath = tempPath;
            this.jobPath = jobPath;
        }
        
        public boolean hasExceptions () {
            return this.hasExceptions;
        }
        
        public Exception lastException () {
            return this.lastException;
        }
        
        public void setCreationFinished () {
            isCreationFinished = true;
        }
        
        public boolean isFinished () {
            return isFinished;
        }
        
        public void addToQueue(String fileName) {
            uploadQueue.add(fileName);
        }
        
        public void run () {
            // keep running until creation is finished
            // and upload queue is empty
            while(isCreationFinished == false || uploadQueue.isEmpty() == false) {
                String fileName = uploadQueue.poll();
                
                // nothing in queue at the moment?
                if (fileName == null) {
                    try {
                        // wait for 1s
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        // don't care
                    }
                    
                    // after waiting move to next iteration
                    continue;
                }     
                
                // we have an active file to upload
                uploadFile(fileName);
                
                // quit when we have exceptions
                if (hasExceptions) break;
            }
            
            // fail?
            if (hasExceptions) {
                return;
            }
            
            // mark upload as finished
            this.isFinished = true;
        }
        
        protected void uploadFile(String fileName) {
            logger.info("Uploading " + fileName + " to HDFS...");
            
            try {
                fs.copyFromLocalFile(false, true, new Path(tempPath + "/" + fileName), new Path(jobPath + "/input/" + fileName));
            } catch (IOException ex) {
                logger.error("Upload failed!", ex);
                hasExceptions = true;
                lastException = ex;
            }
            
            logger.info("Uploaded " + fileName);
        }
    }
    
    class PrepareException extends Exception {
        public PrepareException (String msg) {
            super(msg);
        }
        
        public PrepareException (String msg, Exception ex) {
            super(msg, ex);
        }
    }
        
}
