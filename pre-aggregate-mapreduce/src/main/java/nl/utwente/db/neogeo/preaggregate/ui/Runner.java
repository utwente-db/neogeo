package nl.utwente.db.neogeo.preaggregate.ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Level;
import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class Runner {
    static final Logger logger = Logger.getLogger(Runner.class);
    
    private String cmd;
    
    private Configuration conf;   
    
    private FileSystem fs;
    
    public void run (String[] args) throws IOException, PrepareMR.PrepareException, SQLException, ClassNotFoundException, RunMR.RunException, InterruptedException {
        conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length == 0) {
            throw new IllegalArgumentException("First argument must specify command to run (prepare|run|finish|all)");
        }
        
        // load HDFS filesystem
        fs = FileSystem.get(conf);
        
        cmd = remainingArgs[0].toLowerCase();
        
        if (cmd.equals("prepare")) {
            runPrepare(remainingArgs);
        } else if (cmd.equals("run")) {
            runJob(remainingArgs);
        } else {
            throw new UnsupportedOperationException("Command '" + cmd + " not yet supported");
        }
    }
    
    protected void runJob(String[] args) throws RunMR.RunException, IOException, ClassNotFoundException, InterruptedException {
        logger.info("Running MAPREDUCE phase");
                
        if (args.length != 3) {
            System.err.println("Usage: <config.xml> <jobPath>");
            System.exit(2);
        }
        
        PreAggregateConfig config = loadConfig(args[1]);
        
        String jobPath = args[2];
        
        RunMR run = new RunMR(conf, config);
        run.setFS(fs);
        long execTime = run.doJob(jobPath);
                
        logger.info("Finished MAPREDUCE phase");
        logger.info("Total time: " + execTime + " ms");
        
        // TODO: print next command
    }
    
    protected void runPrepare (String[] args) throws IOException, PrepareMR.PrepareException, SQLException, ClassNotFoundException {
        logger.info("Running PREPARE phase");
        long startTime = System.currentTimeMillis();
        
        if (args.length != 6) {
            System.err.println("Usage: <database.properties> <config.xml> <jobPath> <axis_to_split> <chunk_size>");
            System.exit(2);
        }
        
        DbInfo dbInfo = new DbInfo(args[1]);
        Connection conn = setupConnection(dbInfo);
        
        PreAggregateConfig config = loadConfig(args[2]);
        
        String jobPath = args[3];
        
        int axisToSplitIdx;
        try {
            axisToSplitIdx = Integer.parseInt(args[4]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid axis_to_split index specified; must be a valid integer");
        }
        
        if (axisToSplitIdx >= config.getAxis().length) {
            throw new IllegalArgumentException("Invalid axis_to_split index specified; axis does not exist");
        }
        
        int chunkSize;
        try {
            chunkSize = Integer.parseInt(args[5]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid chunksize specified; must be a valid integer");
        }
        
        if (chunkSize < 1) throw new IllegalArgumentException("Invalid chunksize specified; must be larger than 0");        
       
        long execTime = System.currentTimeMillis() - startTime;
        
        PrepareMR prepare = new PrepareMR(conf, dbInfo, conn, config);
        prepare.setFS(fs);
        long prepareTime = prepare.doPrepare(jobPath, axisToSplitIdx, chunkSize);

        closeConnection(conn);  
                
        logger.info("Finished PREPARE phase");
        logger.info("Total time: " + (execTime + prepareTime) + " ms");
        
        // TODO: print next command
    }
    
    protected void closeConnection (Connection conn) {
        try {
            if (conn.isClosed() == false) {
                conn.close();
            }
        } catch (SQLException ex) {
            logger.warn("Unable to close database connection", ex);
        }
    }
    
    protected Connection setupConnection (DbInfo dbInfo) throws IOException, ClassNotFoundException, SQLException {        
        // load JDBC driver
        Class.forName(dbInfo.getDriverClass());
        
        // build up connection string
        String connUrl = dbInfo.getUrlPrefix() + "://" + dbInfo.getHostname() + ":" + dbInfo.getPort() + "/" + dbInfo.getDatabase();
        
        logger.info("Setting up database connection to " + connUrl);
        Connection conn = DriverManager.getConnection(connUrl, dbInfo.getUsername(), dbInfo.getPassword());
        logger.info("Connection setup!");
        
        return conn;
    }
    
    protected PreAggregateConfig loadConfig (String configFilePath) throws IOException {
        File configFile = new File(configFilePath);
        
        if (configFile.exists() == false) {
            throw new IllegalArgumentException("Invalid config file specified; file path '" + configFilePath + "' does not exist");
        }
        
        PreAggregateConfig aggConf;
        try {
            // test config file
            aggConf = new PreAggregateConfig(configFile);
        } catch (PreAggregateConfig.InvalidConfigException ex) {
            throw new IOException("Unable to read PreAggregate config file", ex);
        }
        
        return aggConf;
    }
    
    public static void main (String[] args) throws Exception {
        logger.info("Started PreAggregate Index Creation Tool for MapReduce");
        
        Runner runner = new Runner();
        runner.run(args);
        
        logger.info("Finished!");
    }
    
    
}
