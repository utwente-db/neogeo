/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.median.generate;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;
import java.util.logging.Level;
import nl.pallett.median.generate.Config.Dataset;
import nl.pallett.median.generate.Config.DbType;
import static nl.pallett.median.generate.Config.DbType.PostgreSQL;
import nl.pallett.median.generate.database.Database;
import nl.pallett.median.generate.database.PostgresDb;
import nl.pallett.median.generate.knmi.GenerateKnmiStructure;
import nl.pallett.median.generate.knmi.GenerateKnmiStructurePostgres;
import nl.pallett.median.generate.pegel.GeneratePegelStructure;
import nl.pallett.median.generate.pegel.GeneratePegelStructurePostgres;
import nl.pallett.median.generate.twitter.GenerateTwitterStructure;
import nl.pallett.median.generate.twitter.GenerateTwitterStructurePostgres;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis
 */
public class Startup {
    private static final Logger log = Logger.getLogger(Startup.class);
    
    public static DecimalFormat formatter;

    protected Config config;
    
    public Startup () {
        formatter = (DecimalFormat) NumberFormat.getInstance(Locale.US);
        DecimalFormatSymbols symbols = formatter.getDecimalFormatSymbols();
        BasicConfigurator.configure();
    }
    
    public void run (String[] args) throws SQLException, IOException {
        log.info("Started logging of the GenerateMedianStructure tool");
        
        config = new Config();
        try {
            config.parseArgs(args);
        } catch (Exception ex) {
            log.error("ERROR: " + ex.getMessage());
            config.printHelp();
            this.exit(1);
        }
        
        Database database = null;
        switch(config.getDatabaseType()) {
            case PostgreSQL:
                database = new PostgresDb();
                break;
            default:
                throw new UnsupportedOperationException("Database type " + config.getDatabaseType() + " is not yet supported");
        }
        
        Dataset dataset = config.getDataset();
        GenerateStructure generator = null;
        
        if (dataset == Dataset.PEGEL) {
            generator = generatePegel();
        } else if (dataset == Dataset.KNMI) {
            generator = generateKnmi();
        } else if (dataset == Dataset.Twitter) {
            generator = generateTwitter();
        } else {
            throw new UnsupportedOperationException("Dataset " + dataset + " not yet supported");
        }      
        
        if (generator != null) {
            generator.setConfig(config);
            generator.setDatabase(database);
            generator.run();
        }
        
        
        log.info("Finished!");
        exit(0);     
    }
    
    protected GenerateStructure generatePegel () throws SQLException, IOException {
        GeneratePegelStructure generator = null;
        
        switch(config.getDatabaseType()) {
            case PostgreSQL:
                generator = new GeneratePegelStructurePostgres ();
                break;
            default:
                throw new UnsupportedOperationException("Database type " + config.getDatabaseType() + " not yet supported for PEGEL");
        }
        
        return generator;
    }
    
    protected GenerateStructure generateKnmi () {
        GenerateKnmiStructure generator = null;
        
        switch(config.getDatabaseType()) {
            case PostgreSQL:
                generator = new GenerateKnmiStructurePostgres ();
        }
        
        return generator;
    }
    
    protected GenerateStructure generateTwitter () {
        GenerateTwitterStructure generator = null;
        
        switch(config.getDatabaseType()) {
            case PostgreSQL:
                generator = new GenerateTwitterStructurePostgres ();
        }
        
        return generator;
    }
    
    protected void exit (int status) {
        log.info("Exiting!");
        System.exit(status);
    }
    
    public static void main (String[] args) throws Exception {
        //args = new String[]{"--dataset", "twitter", "--type", "full", "--database", "twitter", "--table", "twitter_10k_xy", "--split", "1", "--with-values"};
        args = new String[]{"--dataset", "knmi", "--type", "shifting", "--database", "knmi", "--table", "knmi_10k", "--split", "2"};
        
       /*
        args = new String[]{
            "--dataset", "knmi",
            "--database", "knmi",
            "--host", "silo1.ewi.utwente.nl", 
            "-u", "pallet", 
            "--password", "sa",
            "-p", "5431",
            "--type", "shifting",
            "--blocksize", "100k",
            "--table", "knmi_10k",
            "--split", "2"
        };
        */
        
        
        
        
        // run tool
        try {
            (new Startup()).run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
