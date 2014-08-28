package nl.utwente.db.neogeo.preaggregate.ui;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis;
import nl.utwente.db.neogeo.preaggregate.MetricAxis;
import static nl.utwente.db.neogeo.preaggregate.NominalGeoTaggedTweetAggregate.NOMINAL_POSTFIX;
import nl.utwente.db.neogeo.preaggregate.PreAggregate;
import nl.utwente.db.neogeo.preaggregate.SqlUtils;
import nl.utwente.db.neogeo.preaggregate.SqlUtils.DbType;
import nl.utwente.db.neogeo.preaggregate.mapreduce.AggrMapper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Test {

    private static final String CONFIG_FILENAME = "database.properties";
    private String hostname;
    private String port;
    private String username;
    private String password;
    private String database;
    private String schema = "public";
    private String driverClass = "org.postgresql.Driver";
    private String urlPrefix = "jdbc:postgresql";

    private void readProperties() {
        Properties prop = new Properties();
        try {
            InputStream is =
                    this.getClass().getClassLoader().getResourceAsStream(CONFIG_FILENAME);
            prop.load(is);
            hostname = prop.getProperty("hostname");
            port = prop.getProperty("port");
            username = prop.getProperty("username");
            password = prop.getProperty("password");
            database = prop.getProperty("database");

            if (prop.containsKey("schema")) {
                schema = prop.getProperty("schema");
            }

            if (prop.containsKey("driver")) {
                driverClass = prop.getProperty("driver");
            }

            if (prop.containsKey("url_prefix")) {
                urlPrefix = prop.getProperty("url_prefix");
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public String getSchema() {
        return this.schema;
    }

    public String getHostname() {
        return this.hostname;
    }

    public String getPort() {
        return this.port;
    }

    public String getUsername() {
        return this.username;
    }

    public String getPassword() {
        return this.password;
    }

    public String getDatabase() {
        return this.database;
    }

    public Connection getConnection() {
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your JDBC Driver (" + driverClass + ")? "
                    + "Include in your library path!");
            e.printStackTrace();
            return null;
        }
        System.out.println("JDBC Driver (" + driverClass + ") Registered!");

        // build up connection string
        String connUrl = urlPrefix + "://" + hostname + ":" + port + "/" + database;

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connUrl, username, password);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return null;
        }

        if (connection != null) {
            System.out.println("You made it, take control your database now!");
        } else {
            System.out.println("Failed to make connection!");
        }
        return connection;
    }

    public static void main(String[] argv) throws Exception {
        System.out.println("Test pre-aggregate package");
        Test t = new Test();
        t.readProperties();
        Connection connection = t.getConnection();

        if (connection == null) {
            System.err.println("Unable to create connection object!");
            System.exit(1);
        }

        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.DEBUG);

        //runTest_create_chunks(connection, t.getSchema());

        //runTest_mapper();

        //runTest_MapReduce(connection, t.getSchema());
        
        //runTest_prepare(connection, t.getSchema());

        runTest_copyOut(connection, t);

        connection.close();
    }

    public static void runTest_copyOut(Connection c, Test test) throws Exception {
        Statement q = c.createStatement();

        MapiSocket server = new MapiSocket();

        server.setDatabase(test.getDatabase());
        server.setLanguage("sql");

        List warning = server.connect(test.getHostname(), Integer.parseInt(test.getPort()), test.getUsername(), test.getPassword());
        if (warning != null) {
            for (Iterator it = warning.iterator(); it.hasNext();) {
                System.out.println(it.next().toString());
            }
        }

        BufferedMCLReader in = server.getReader();
        BufferedMCLWriter out = server.getWriter();

        String error = in.waitForPrompt();
        if (error != null) {
            throw new Exception(error);
        }

        
        // the leading 's' is essential, since it is a protocol
        // marker that should not be omitted, likewise the
        // trailing semicolon
        out.write('s');
        
        String line;
        /*
        while((line = in.readLine()) != null) {
            // when PROMPT is reached all data has been read
            int lineType = in.getLineType();            
            if (lineType == BufferedMCLReader.PROMPT) break;
        }
        */
        //while((line = in.readLine()) != null) {
            //System.out.println(line);
        //}
        
        String query = "COPY SELECT * FROM test INTO STDOUT USING DELIMITERS ',','\\n';";
        out.write(query);
        out.newLine();
        out.writeLine("");
        
        
        while((line = in.readLine()) != null) {
            int lineType = in.getLineType();

            // when PROMPT is reached all data has been read
            if (lineType == BufferedMCLReader.PROMPT) break;
            
            // ignore all other official lines
            if (lineType != 0) continue;
            
            System.out.println(line);
        }       
        
        error = in.waitForPrompt();
	if (error != null) throw new Exception(error);
        
        out.write('s');
        
        query = "COPY SELECT * FROM test INTO STDOUT USING DELIMITERS ',','\\n';";
        out.write(query);
        out.newLine();
        out.writeLine("");
        
        
        while((line = in.readLine()) != null) {
            int lineType = in.getLineType();

            // when PROMPT is reached all data has been read
            if (lineType == BufferedMCLReader.PROMPT) break;
            
            // ignore all other official lines
            if (lineType != 0) continue;
            
            System.out.println(line);
        }  
        
        server.close();
        System.out.println("Finished");
    }

  

    
    
    public static void runTest_prepare(Connection c, String schema) throws Exception {
        double DFLT_BASEBOXSIZE = 0.001;
        short DFLT_N = 4;
        //GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, "public", "uk_neogeo", "myAggregate", "coordinates",-1,200000,null);

        AggregateAxis axis[] = null;
        if (SqlUtils.dbType(c) == DbType.MONETDB) {
            axis = new AggregateAxis[]{
                new MetricAxis("coordinates_x", "double", "" + DFLT_BASEBOXSIZE, DFLT_N),
                new MetricAxis("coordinates_y", "double", "" + DFLT_BASEBOXSIZE, DFLT_N), //new MetricAxis("\"time\"", "timestamp with time zone", "360000" /*=10 min*/, (short)16)
            };
        } else {
            axis = new AggregateAxis[]{
                new MetricAxis("ST_X(coordinates)", "double", "" + DFLT_BASEBOXSIZE, DFLT_N),
                new MetricAxis("ST_Y(coordinates)", "double", "" + DFLT_BASEBOXSIZE, DFLT_N), //new MetricAxis("time", "timestamp with time zone", "360000" /*=10 min*/, (short)16)
            };
        }
        
        //PrepareMR prepare = new PrepareMR(new Configuration(), c, schema, "london_hav_neogeo", null /*override_name*/, "myAggregate", axis, "len" , "bigint", PreAggregate.AGGR_ALL);
        //prepare.doPrepare("/data/london_hav_neogeo/", 0, 2000);
    }

    
}
