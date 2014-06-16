/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis
 */
public class Config {
    private static final Logger log = Logger.getLogger(Config.class);
    
    public enum Type {
        SHIFTING, FULL
    }
    
    public enum Dataset {
        PEGEL, KNMI, Twitter
    }
    
    public enum DbType {
        PostgreSQL
    }
    
    public static final int BATCH_SIZE = 50;
    
    public static final int BLOCK_SIZE = 100000;
    
    protected Dataset dataset = null;
    
    protected DbType databaseType = DbType.PostgreSQL;
    
    protected String user = "postgres";
    
    protected String password = "sa";
    
    protected String database = "pegel";
    
    protected String host = "localhost";
    
    protected int port = 5432;
    
    protected String table;
    
    protected int splitFactor = 1;
    
    protected Type type = Type.SHIFTING;
    
    protected int blockSize = BLOCK_SIZE;
    
    protected Options options;
    
    protected boolean datastreamsOnly = false;
    
    protected boolean withValues = false;
    
    public Config () {
        this.buildOptions();
    }
    
    public void printHelp () {
       HelpFormatter formatter = new HelpFormatter();
       formatter.printHelp("generate-structure", options); 
    }
    
    public void parseArgs (String[] args) throws Exception {
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
                cmd = parser.parse(options, args);
        }
        catch (ParseException e) {
            throw new Exception(e.getMessage());
        }
        
        if (cmd == null) return;
        
        // get type
        String type = cmd.getOptionValue("type").toLowerCase();
        if (type.equals("shifting")) {
            this.type = Type.SHIFTING;
        } else  if (type.equals("full")) {
            this.type = Type.FULL;
        } else {
            throw new Exception("Invalid type specified! Must be either 'shifting' or 'full'");
        }      
        
        // get dataset
        String dataset = cmd.getOptionValue("dataset").toLowerCase();
        if (dataset.equals("pegel")) {
            this.dataset = Dataset.PEGEL;
        } else if (dataset.equals("knmi")) {
            this.dataset = Dataset.KNMI;
        } else if (dataset.equals("twitter")) {
            this.dataset = Dataset.Twitter;
        } else {
            throw new Exception("Invalid dataset specified! Must be pegel, knmi or twitter");
        }
        log.info("Dataset: " + dataset);
        
        // get database type
        String dbType = cmd.getOptionValue("dbtype");
        if (dbType != null && dbType.length() > 0) {
            dbType = dbType.toLowerCase();
            if (dbType.equals("postgres") || dbType.equals("postgresql")) {
                this.databaseType = DbType.PostgreSQL;
            } else {
                throw new Exception("Invalid database type specified");
            }
        }
        log.info("Database type: " + databaseType);
        
        // get table name
        this.table = cmd.getOptionValue("table");
        log.info("Preparing table: " + this.getTable());
        
        // get DB host
        if (cmd.hasOption("host") && cmd.getOptionValue("host").isEmpty() == false) {
            this.host = cmd.getOptionValue("host");
            log.info("Database host: " + getHost());
        }
        
        // get DB name
        if (cmd.hasOption("database") && cmd.getOptionValue("database").isEmpty() == false) {
            this.database = cmd.getOptionValue("database");
            log.info("Database name: " + getDatabase());
        }
        
        // get DB user
        if (cmd.hasOption("user") && cmd.getOptionValue("user").isEmpty() == false) {
            this.user = cmd.getOptionValue("user");
            log.info("Database user: " + getUser());
        }
        
        // get DB password
        if (cmd.hasOption("password") && cmd.getOptionValue("password").isEmpty() == false) {
            this.password = cmd.getOptionValue("password");
        }
        
        String splitFactorString = cmd.getOptionValue("split");
        try {
            splitFactor = Integer.parseInt(splitFactorString);
        } catch (NumberFormatException e) {
            splitFactor = 1;
        }
        log.info("Using split factor: " + getSplitFactor());
        
        String blockSizeString = cmd.getOptionValue("blocksize");
        if (blockSizeString != null) {
            blockSizeString = blockSizeString.toLowerCase();
            try {
                if (blockSizeString.endsWith("k")) {
                    blockSize = Integer.parseInt(blockSizeString.substring(0, blockSizeString.length()-1)) * 1000;
                } else if (blockSizeString.endsWith("mm")) {
                    blockSize = Integer.parseInt(blockSizeString.substring(0, blockSizeString.length()-2)) * 1000000;
                } else {
                    blockSize = Integer.parseInt(blockSizeString);
                }
            } catch (NumberFormatException e) {
                log.warn("Invalid blocksize specified, using default blocksize");
            }
        }
        log.info("Block size: " + getBlockSize());       
        
        String portString = cmd.getOptionValue("port");
        if (portString != null && portString.isEmpty() == false) {
            try {
                port = Integer.parseInt(portString);
                log.info("Database port: " + getPort());
            } catch (NumberFormatException e) {
                log.warn("Invalid port specified; must be a number");
            }
            
        }
        
        if (cmd.hasOption("data-streams-only")) {
            datastreamsOnly = true;
            log.info("Only generating datastreams");
        }
        
        if (cmd.hasOption("with-values")) {
            withValues = true;
            log.info("Including values in datastreams");
        }
    }
    
    protected void buildOptions () {
        options = new Options();   
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(true);
        OptionBuilder.withDescription("Specify the dataset to generate a structure for");
        OptionBuilder.withLongOpt("dataset");
        options.addOption(OptionBuilder.create());
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(false);
        OptionBuilder.withDescription("Specify the database type");
        OptionBuilder.withLongOpt("dbtype");
        options.addOption(OptionBuilder.create());
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(true);
        OptionBuilder.withDescription("Specify the type of median structure to generate");
        OptionBuilder.withLongOpt("type");
        options.addOption(OptionBuilder.create());
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(true);
        OptionBuilder.withDescription("Specify the table to generate median structure for");
        OptionBuilder.withLongOpt("table");
        options.addOption(OptionBuilder.create("t"));
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(false);
        OptionBuilder.withDescription("Specify the host or IP address of the database server");
        OptionBuilder.withLongOpt("host");
        options.addOption(OptionBuilder.create("h"));
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(false);
        OptionBuilder.withDescription("Specify the name of the database");
        OptionBuilder.withLongOpt("database");
        options.addOption(OptionBuilder.create("d"));
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(false);
        OptionBuilder.withDescription("Specify the database user");
        OptionBuilder.withLongOpt("user");
        options.addOption(OptionBuilder.create("u"));
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(false);
        OptionBuilder.withDescription("Specify the password of the database user");
        OptionBuilder.withLongOpt("password");
        options.addOption(OptionBuilder.create());
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(false);
        OptionBuilder.withDescription("Specify the port of the database server");
        OptionBuilder.withLongOpt("port");
        OptionBuilder.withType(Number.class);
        options.addOption(OptionBuilder.create("p"));
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(false);
        OptionBuilder.withDescription("Specify the factor to split the table into, e.g. 1, 2, 4, 10, etc");
        OptionBuilder.withLongOpt("split");
        OptionBuilder.withType(Number.class);
        options.addOption(OptionBuilder.create("s"));
        
        OptionBuilder.hasArg(true);
        OptionBuilder.isRequired(false);
        OptionBuilder.withDescription("Specify the blocksize of the data blocks, e.g. 100k, 250k, etc");
        OptionBuilder.withLongOpt("blocksize");
        OptionBuilder.withType(Number.class);
        options.addOption(OptionBuilder.create("b"));
        
        OptionBuilder.hasArg(false);
        OptionBuilder.isRequired(false);
        OptionBuilder.withDescription("Specify whether to only generate the data streams");
        OptionBuilder.withLongOpt("data-streams-only");
        options.addOption(OptionBuilder.create());
        
        OptionBuilder.hasArg(false);
        OptionBuilder.isRequired(false);
        OptionBuilder.withDescription("Specify whether to only include the actual values in the data streams");
        OptionBuilder.withLongOpt("with-values");
        options.addOption(OptionBuilder.create());
    }

    public Dataset getDataset() {
        return dataset;
    }

    public DbType getDatabaseType() {
        return databaseType;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getTable() {
        return table;
    }

    public int getSplitFactor() {
        return splitFactor;
    }

    public Type getType() {
        return type;
    }

    public int getBlockSize() {
        return blockSize;
    }
    
    public boolean datastreamsOnly () {
        return this.datastreamsOnly;
    }
    
    public boolean withValues () {
        return this.withValues;
    }
    
}
