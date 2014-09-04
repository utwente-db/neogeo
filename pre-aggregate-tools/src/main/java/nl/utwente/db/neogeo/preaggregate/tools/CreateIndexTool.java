package nl.utwente.db.neogeo.preaggregate.tools;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis;
import nl.utwente.db.neogeo.preaggregate.PreAggregate;
import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class CreateIndexTool {
    private static final Logger logger = LogManager.getLogger(CreateIndexTool.class.getSimpleName());
    
    protected Options options;
    
    protected CommandLine cli;
    
    protected Connection conn;
    
    protected int axisToSplit = -1;
    
    protected int chunkSize = -1;
    
    protected String configFileName;
    
    protected PreAggregateConfig aggConfig = null;
    
    protected String dbName;
    
    protected String dbType;
    
    protected String host;
    
    protected int port;
    
    protected String schema;
    
    protected String dbUser;
    
    protected String password;
    
    protected String jdbcPrefix;
    
    protected String jdbcDriver;
    
    protected int returnStatus = 0;
    
    protected boolean verbose = false;
    
    protected ConsoleAppender console;
    
    public CreateIndexTool () {
        initLogger();
        initCliOptions();
    }
    
    protected void initLogger() {
        Logger.getRootLogger().getLoggerRepository().resetConfiguration();
        
        console = new ConsoleAppender(); //create appender
        //configure the appender
        String PATTERN = "[%d{HH:mm:ss,SSS}] %C{1} - %p: %m%n";
        
        console.setLayout(new PatternLayout(PATTERN)); 
        console.setThreshold(Level.INFO);
        console.activateOptions();
        
        //add appender to any Logger (here is root)
        Logger.getRootLogger().addAppender(console);
    }
    
    public Options getOptions () {
        return this.options;
    }
    
    public int getReturnStatus () {
        return returnStatus;
    }
    
    public void run (String[] args) {
        logger.info("Started NeoGeo PreAggregate Index Creation Tool");
        
        if (parseCliArgs(args) == false) return;
        
        if (setupConnection() == false) return;
        
        if (createIndex() == false) return;
        
        
        closeConnection();
        
        
        logger.info("Creation Tool Finished");
    }
    
    protected void closeConnection () {
        if (conn != null) {
            try {
                if (conn.isClosed() == false) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.warn("Unable to properly close database connection");
            }
            
            logger.info("Database connection closed properly");
        }
    }
    
    protected boolean parseCliArgs (String[] args) {
        HelpFormatter formatter = new HelpFormatter();
        
        // print help?
        if (args.length >= 1 && args[0].toLowerCase().endsWith("help")) {
            formatter.printHelp("create-pa-index", this.getOptions());
            return false;
        }
        
        CommandLineParser parser = new BasicParser();
        
        try { 
            cli = parser.parse(options, args);
        } catch (ParseException ex) {
            logger.error(ex.getMessage());
            formatter.printHelp("create-pa-index", getOptions());   
            return false;
        }
        
        verbose = cli.hasOption("verbose");
        if (verbose) {
            console.setThreshold(Level.DEBUG);
            logger.debug("Output set to VERBOSE");
        }
        
        this.configFileName = cli.getOptionValue("config");
        
        try {
            aggConfig = new PreAggregateConfig(new File(this.configFileName));
        } catch (PreAggregateConfig.InvalidConfigException ex) {
            logger.error("Invalid PreAggregate config file specified: " + ex.getMessage());
            return false;
        }
        
        host = cli.getOptionValue("host");
        if (host == null || host.isEmpty()) {
            logger.error("Host name or IP address cannot be empty");
            return false;
        }
        
        dbName = cli.getOptionValue("database");
        if (dbName == null || dbName.isEmpty()) {
            logger.error("Database name cannot be empty");
            return false;
        }
        
        dbType = cli.getOptionValue("dbtype");
        if (dbType == null || dbType.isEmpty()) {
            logger.error("Database type cannot be empty");
            return false;
        }
        
        dbType = dbType.toLowerCase();
        
        // try to automatically determine additional db options
        if (dbType.startsWith("postgres")) {
            schema = "public";
            port = 5432;
            jdbcPrefix = "jdbc:postgresql";
            jdbcDriver = "org.postgresql.Driver";
        } else if (dbType.startsWith("monet")) {
            schema = "sys";
            port = 50000;
            jdbcPrefix = "jdbc:monetdb";
            jdbcDriver = "nl.cwi.monetdb.jdbc.MonetDriver";
        }
        
        dbUser = cli.getOptionValue("user");
        if (dbUser == null || dbUser.isEmpty()) {
            logger.error("Database user cannot be empty");
            return false;
        }
        
        String portStr = cli.getOptionValue("port");
        if (portStr != null && portStr.isEmpty() == false) {
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                logger.error("Port must be a valid integer");
                return false;
            }
        }
        
        String schemaNew = cli.getOptionValue("schema");
        if (schemaNew != null && schemaNew.isEmpty() == false) {
            schema = schemaNew;
        }
        
        password = cli.getOptionValue("password");
        if (password == null || password.isEmpty()) {
            // TODO: in the future simply prompt user for password
            logger.error("Database password cannot be empty");
            return false;
        }
        
        String axisToSplitStr = cli.getOptionValue("axistosplit");
        if (axisToSplitStr != null && axisToSplitStr.isEmpty() == false) {
            try {
                axisToSplit = Integer.parseInt(axisToSplitStr);
                
                if (axisToSplit >= aggConfig.getAxis().length) {
                    logger.error("Index specified by AxisToSplit is too high; must be a valid axis index");
                    return false;
                }
            } catch (NumberFormatException e) {
                logger.error("AxisToSplit must be a valid integer, specifing the index of an axis");
                return false;
            }
        }
        
        String chunkSizeStr = cli.getOptionValue("chunksize");
        if (chunkSizeStr != null && chunkSizeStr.isEmpty() == false) {
            try {
                chunkSize = Integer.parseInt(chunkSizeStr);
                
                if (chunkSize < 1) {
                    logger.error("The chunksize specified is too small");
                    return false;
                }                
            } catch (NumberFormatException e) {
                logger.error("Chunksize must be a valid integer");
                return false;                        
            }
        }
        
        if (axisToSplit != -1 && chunkSize == -1) {
            logger.error("You have specified an AxisToSplit but not a chunksize. Please specify both or neither");
            return false;
        }
        
        if (axisToSplit == -1 && chunkSize != -1) {
            logger.error("You have specified a chunksize but not an AxisToSplit. Please specify both or neither");
            return false;
        }        
        
        return true;        
    }
    
    protected boolean createIndex () {
        long startTime = System.currentTimeMillis();
        logger.info("Creating PreAggregate index for table " + this.schema + "." + aggConfig.getTable() + "");
        
        AggregateAxis[] axis = aggConfig.getAxis();
        
        if (axisToSplit != -1) {
            logger.info("Splitting axis " + axisToSplit + ": " + axis[axisToSplit].columnExpression() + " into chunks of " + chunkSize + " rows");
        }
        
        try {
            PreAggregate pa = new PreAggregate(
                conn, 
                schema, 
                aggConfig.getTable(), 
                null /*override_name*/,
                aggConfig.getLabel(),
                axis, 
                aggConfig.getAggregateColumn(),
                aggConfig.getAggregateType(), 
                aggConfig.getAggregateMask(), 
                axisToSplit, 
                chunkSize, 
                null
            );
        } catch (SQLException ex) {
            logger.fatal("Error during index creation: " + ex.getMessage());
            logger.debug("SQLException", ex);
            return false;
        }
        
        long execTime = System.currentTimeMillis() - startTime;
        logger.info("Finished creating index in " + execTime + " ms");
        return true;
    }
    
    protected boolean setupConnection () {
        logger.info("Setting up database connection...");
        
        try {
            Class.forName(jdbcDriver);
        } catch (ClassNotFoundException e) {
            logger.error("Unable to load JDBC Driver (" + jdbcDriver + "). Make sure it is in the classpath.");
            return false;
        }
        
        // build up connection string
        String connUrl = jdbcPrefix + "://" + host + ":" + port + "/" + dbName;
        
        logger.info("Connection URL: " + connUrl + " (with user '" + dbUser + "')");

        try {
            conn = DriverManager.getConnection(connUrl, dbUser, password);
        } catch (SQLException e) {
            logger.error("Unable to setup database connection: " + e.getMessage());
            logger.debug("SQLException", e);
            return false;
        }        
        
        logger.info("Database connection set up!");
        return true;
    }
    
    protected void initCliOptions () {
        options = new Options();
        
        options.addOption(
                OptionBuilder.withDescription("Enable verbose output logging")
                .withLongOpt("verbose")
                .create("v")
        );
        
        options.addOption(
                OptionBuilder.withArgName("file")
                .hasArg()
                .withDescription("PreAggregate XML config file")
                .isRequired()
                .create("config")
        );
                
        options.addOption(
                OptionBuilder.withArgName("postgresql|monetdb")
                .hasArg()
                .withDescription("type of database")
                .isRequired()
                .create("dbtype")
        );
        
        options.addOption(
                OptionBuilder.withArgName("host")
                .hasArg()
                .withDescription("database host name or ip address")
                .withLongOpt("host")
                .isRequired()
                .create("h")
        );
        
        options.addOption(
                OptionBuilder.withArgName("port")
                .hasArg()
                .withDescription("port number of the database")
                .withLongOpt("port")
                .create("p")
        );
        
        options.addOption(
                OptionBuilder.withArgName("user")
                .hasArg()
                .withDescription("database username")
                .withLongOpt("user")
                .isRequired()
                .create("u")
        );
        
        options.addOption(
                OptionBuilder.withArgName("password")
                .hasArg()
                .withDescription("database password")
                .isRequired()
                .create("password")
        );
    
        options.addOption(
                OptionBuilder.withArgName("dbname")
                .hasArg()
                .withDescription("name of database")
                .withLongOpt("database")
                .isRequired()
                .create("d")
        );
        
        options.addOption(
                OptionBuilder.withArgName("schema")
                .hasArg()
                .withDescription("schema name in the database")
                .withLongOpt("schema")
                .create("s")
        );        
        
        options.addOption(
                OptionBuilder.withArgName("axis index")
                .hasArg()
                .withDescription("index of axis to split")
                .create("axistosplit")
        );
        
        options.addOption(
                OptionBuilder.withArgName("size")
                .hasArg()
                .withDescription("maximum size of chunk after splitting axis")
                .create("chunksize")
        );
  
        options.addOption(
                OptionBuilder.withDescription("prints this help message")
                .create("help")
        );
    }    
    
    public static void main (String[] args) {        
        CreateIndexTool tool = new CreateIndexTool();
        tool.run(args);
        
        System.exit(tool.getReturnStatus());
    }
    
}
