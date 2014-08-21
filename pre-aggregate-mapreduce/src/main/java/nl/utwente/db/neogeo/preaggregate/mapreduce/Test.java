package nl.utwente.db.neogeo.preaggregate.mapreduce;

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
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis;
import nl.utwente.db.neogeo.preaggregate.MetricAxis;
import static nl.utwente.db.neogeo.preaggregate.NominalGeoTaggedTweetAggregate.NOMINAL_POSTFIX;
import nl.utwente.db.neogeo.preaggregate.PreAggregate;
import nl.utwente.db.neogeo.preaggregate.SqlUtils;
import nl.utwente.db.neogeo.preaggregate.SqlUtils.DbType;
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
        
        public String getSchema () {
            return this.schema;
        }
        
	public Connection getConnection(){            
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
                
                //BasicConfigurator.configure();
                Logger.getRootLogger().setLevel(Level.DEBUG);
		                                
                //runTest_create_chunks(connection, t.getSchema());
                
                //runTest_mapper();
                
                runTest_MapReduce(connection, t.getSchema());
            
                connection.close();
	}
        
        public static void runTest_MapReduce (Connection c, String schema) throws SQLException, IOException, InterruptedException {
            CreateIndexMR job = new CreateIndexMR();
            
            String[] args = new String[]{"D:\\downloads\\preaggregate.xml", "/chunk_*.csv", "/out"};
            
            job.parseArgs(args);
            
            
            //job.runJob();
        }
        
        public static void runTest_mapper () throws Exception {
            double	DFLT_BASEBOXSIZE = 0.001;
            int 	DFLT_N = 4;   
        
            AggrMapper mapper = new IntAggrMapper();            
            
            Configuration conf = new Configuration();
            
            conf.set("aggregate_type", "int");
            
            conf.setInt("aggregate_mask", PreAggregate.AGGR_ALL);
            
            conf.setInt("axis_count", 2);
            
            conf.set("axis_0_class", "MetricAxis");
            conf.set("axis_0_column", "coordinates_x");
            conf.set("axis_0_type", "double");
            conf.set("axis_0_low", "-0.12");
            conf.set("axis_0_high", "0.449");
            conf.set("axis_0_baseblocksize", String.valueOf(DFLT_BASEBOXSIZE));
            conf.setInt("axis_0_n", DFLT_N);
            
            conf.set("axis_1_class", "MetricAxis");
            conf.set("axis_1_column", "coordinates_y");
            conf.set("axis_1_type", "double");
            conf.set("axis_1_low", "51.327");
            conf.set("axis_1_high", "51.658");
            conf.set("axis_1_baseblocksize", String.valueOf(DFLT_BASEBOXSIZE));
            conf.setInt("axis_1_n", DFLT_N);
            
            mapper.setConfiguration(conf);
            
            mapper.setup(null);
            
            BytesWritable value = new BytesWritable();
            byte[] contents = FileUtils.readFileToByteArray(new File("D:\\Downloads\\chunks\\chunk_0.csv"));
            
            value.set(contents, 0, contents.length);
            
            //mapper.map(null, value, null);
            
            mapper.cleanup(null);
        }
               

        public static void runTest_create_chunks(Connection c, String schema) throws Exception {
		double	DFLT_BASEBOXSIZE = 0.001;
		short	DFLT_N = 4;
		//GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, "public", "uk_neogeo", "myAggregate", "coordinates",-1,200000,null);
                
                AggregateAxis axis[] = null;
                if (SqlUtils.dbType(c) == DbType.MONETDB) {
                    axis = new AggregateAxis[]{
                            new MetricAxis("coordinates_x", "double", "" + DFLT_BASEBOXSIZE,DFLT_N),
                            new MetricAxis("coordinates_y", "double", "" + DFLT_BASEBOXSIZE,DFLT_N),
                            //new MetricAxis("\"time\"", "timestamp with time zone", "360000" /*=10 min*/, (short)16)
                    };
                } else {                
                    axis = new AggregateAxis[]{
                            new MetricAxis("ST_X(coordinates)", "double", "" + DFLT_BASEBOXSIZE,DFLT_N),
                            new MetricAxis("ST_Y(coordinates)", "double", "" + DFLT_BASEBOXSIZE,DFLT_N),
                            //new MetricAxis("time", "timestamp with time zone", "360000" /*=10 min*/, (short)16)
                    };
                }
                
                CreateChunks obj = new CreateChunks(c, schema);
                
                obj.create("london_hav_neogeo", axis, "len", "bigint", PreAggregate.AGGR_ALL, 0, 2000, "/home/dbguest/chunks/");
                
                //obj.create("uk_neogeo", axis, "len", "bigint", PreAggregate.AGGR_ALL, 0, 200000, "/home/dbguest/chunks/");
                
		//PreAggregate pa = new PreAggregate(c,"public", "uk_neogeo", null /*override_name*/, "myAggregate",axis,"char_length(tweet)","bigint",PreAggregate.AGGR_ALL,2,200000,null);
                //PreAggregate pa = new PreAggregate(c, schema, "london_hav_neogeo", null /*override_name*/, "myAggregate", axis, "len" , "bigint", PreAggregate.AGGR_ALL, 2, 200000, null);
	}
        
        
	
}
