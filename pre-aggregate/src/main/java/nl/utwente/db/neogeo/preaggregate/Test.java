package nl.utwente.db.neogeo.preaggregate;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import nl.utwente.db.neogeo.preaggregate.SqlUtils.DbType;

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
                
		// runTest_nominal( connection );
		// setup_silo3( connection );
		//runTest_time(connection);
                
                runTest3(connection, t.getSchema());
                
                connection.close();
	}
	
	@SuppressWarnings("deprecation")
	public static void runTest_time(Connection c) throws Exception {
		SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yy-MM-dd:HH:mm:SS");
		Date from = DATE_FORMAT.parse("2004-09-15:23:57:36");
		Timestamp ts_from = new Timestamp(from.getTime() );
		Date to = DATE_FORMAT.parse("2014-01-06:00:00:57");
		Timestamp ts_to = new Timestamp(to.getTime() );
		Date split = DATE_FORMAT.parse("2014-01-06:00:00:57");
		//Timestamp ts_split = new Timestamp(split.getTime() );
		//System.out.println("ts_from="+ts_from);
		System.out.println("ts_to  ="+ts_to);
		AggregateAxis time_axis = new MetricAxis("time","timestamp with time zone",ts_from,ts_to,"86400",(short)4);
		System.out.println("TIME-AXIS="+time_axis);
		System.out.println("TIME-SPLIT="+time_axis.splitAxis(ts_from,ts_to,1));
		
	}

	public static void setup_silo3(Connection c) throws Exception {
		try {
			System.out.println("Setting up silo3 again!");
			GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, "public", "uk_neogeo", null, "myAggregate", "coordinates",1 /* axis 2 split*/,200000,null);
			c.close();
		} catch (SQLException e) {
			System.out.println("Caught: " + e);
			e.printStackTrace(System.out);
		}
		System.out.println("#!finished");
	}
	
        public static void printObjectArray(Object[][] obj) {
           for(int i=0; i < obj.length; i++) {
                for(int j=0; j < obj[i].length; j++) {
                    System.out.println("obj[" + i + "][" + j + "] = " + obj[i][j]);
                }
            } 
        }
        
	public static void runTest2(Connection c, String schema) throws Exception {
		try {
			// new TweetConverter(c,"public","london_hav_raw",c,"public","london_hav");
			// new TweetConverter("/Users/flokstra/twitter_sm.db",c,"public","london_hav");
			// new TweetConverter("/Users/flokstra/uk_raw.sql",c,"public","uk");
			//

                        GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, schema, "london_hav_neogeo", null, "myAggregate", "coordinates",0 /* axis 2 split*/,200000,null);
			//GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, schema, "london_hav_neogeo", "myAggregate"); 
                    
                        //GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, schema, "uk_neogeo", "myAggregate"); 
                        
			Object[][] obj_range = pa.getRangeValues(c);
			//			ResultSet rs = pa.SQLquery(PreAggregate.AGGR_COUNT, obj_range);
                        
                        /*
                        printObjectArray(obj_range);
                        System.exit(0);
                        */
                        
                                                                      
                        /*
			int[] range = new int[3];
			range[0] = 3;
			range[1] = 4;
			range[2] = 1;
			Object[][] iv_first_obj = new Object[3][2];
			iv_first_obj[0][0] = Math.floor(((Double)obj_range[0][0])/0.001)*0.001;
			iv_first_obj[0][1] = ((Double)iv_first_obj[0][0])+Math.ceil((((Double)obj_range[0][1]) - ((Double)obj_range[0][0]))/3/0.001)*0.001;
			iv_first_obj[1][0] = Math.floor(((Double)obj_range[1][0])/0.001)*0.001;
			iv_first_obj[1][1] = ((Double)iv_first_obj[1][0])+Math.ceil((((Double)obj_range[1][1]) - ((Double)obj_range[1][0]))/4/0.001)*0.001;
			iv_first_obj[2][0] = new Timestamp(((Double)(Math.floor(((Timestamp)obj_range[2][0]).getTime()/3600000.0)*3600000)).longValue()); 
			iv_first_obj[2][1] = new Timestamp(((Double)(Math.ceil(((Timestamp)obj_range[2][1]).getTime()/3600000.0)*3600000)).longValue());
                        */
                        
                        int[] range = new int[2];
			range[0] = 4;
			range[1] = 4;
			Object[][] iv_first_obj = new Object[2][2];
			iv_first_obj[0][0] = Math.floor(((Double)obj_range[0][0])/0.001)*0.001;
			iv_first_obj[0][1] = ((Double)iv_first_obj[0][0])+Math.ceil((((Double)obj_range[0][1]) - ((Double)obj_range[0][0]))/3/0.001)*0.001;
			iv_first_obj[1][0] = Math.floor(((Double)obj_range[1][0])/0.001)*0.001;
			iv_first_obj[1][1] = ((Double)iv_first_obj[1][0])+Math.ceil((((Double)obj_range[1][1]) - ((Double)obj_range[1][0]))/4/0.001)*0.001;
                        
                        /*
                        printObjectArray(iv_first_obj);
                        System.exit(0);
                        */
                        
                        
                        ResultSet rs = null;
                        
                        
			rs = pa.SQLquery_grid(PreAggregate.AGGR_COUNT, iv_first_obj, range);
			while(rs.next()){
				System.out.println(rs.getInt(1)+"|"+rs.getLong(2) + "|" + rs.getLong(3));
			}
			rs.close();
                        
                        System.exit(0);
                        
                        
                         
                        
                        
			System.out.println("\n\n standard query!");
			rs = pa.SQLquery_grid_standard(PreAggregate.AGGR_COUNT, iv_first_obj, range);
			while(rs.next()){
				System.out.println(rs.getInt(1)+"|"+rs.getLong(2));
			}
			rs.close();
                        
                        System.exit(0);
                        
                        
			System.out.println("\n\n with splitting!");
			int i=0;
			for(AggregateAxis a_base : pa.getAxis()){
				MetricAxis a = null;
				
				if ( a.isMetric() )
					a = (MetricAxis)a_base;
				else
					throw new SQLException("Cannot split over non-metric axis");
				AxisSplitDimension dim = a.splitAxis(a.low(), a.high(), range[i]);
				if(dim==null) throw new Exception("query area out of available data domain");
				range[i] = dim.getCount();
				iv_first_obj[i][0] = dim.getStart();
				iv_first_obj[i][1] = dim.getEnd();
				i++;
			}
			rs = pa.SQLquery_grid(PreAggregate.AGGR_COUNT, iv_first_obj, range);
			while(rs.next()){
				System.out.println(rs.getInt(1)+"|"+rs.getLong(2));
			}

			rs.close();
			System.out.println("\n\n execute directly!");
			i=0;
			String sql_sel = "select ";
			String sql_constr = " where ";
			String sql_group = " group by ";
			System.out.println("cnt | low | factor | high");
			for(AggregateAxis a : pa.getAxis()){
				if(iv_first_obj[i][1] instanceof Double){
					double start = (Double)iv_first_obj[i][0];
					double end = (Double)iv_first_obj[i][1];
					System.out.print(range[i]+"|"+iv_first_obj[i][0]+"|");
					System.out.print((end-start)+"|");
					System.out.println(start+(end-start)*range[i]);
					if(range[i]>1){
						sql_sel += " floor("+a.columnExpression()+"/"+ (end-start)+") as a"+i+",";
						sql_group += " floor("+a.columnExpression()+"/"+ (end-start)+") ,";
					}
					sql_constr += " "+a.columnExpression()+">="+start+" and "+a.columnExpression()+"<="+(start+(end-start)*range[i])+" and ";
				}
				if(iv_first_obj[i][1] instanceof Timestamp){
					long start = ((Timestamp)iv_first_obj[i][0]).getTime()/1000;
					long end = ((Timestamp)iv_first_obj[i][1]).getTime()/1000;
					System.out.print(range[i]+"|"+start+"|");
					System.out.print((end-start)+"|");
					System.out.println(start+(end-start)*range[i]);
					if(range[i]>1){
						sql_sel += " EXTRACT(EPOCH FROM "+a.columnExpression()+")/"+ (end-start)+" as a"+i+",";
						sql_group += " EXTRACT(EPOCH FROM "+a.columnExpression()+")/"+ (end-start)+" ,";
					}
					sql_constr += " EXTRACT(EPOCH FROM "+a.columnExpression()+")>="+start+" and EXTRACT(EPOCH FROM "+a.columnExpression()+")<="+(start+(end-start)*range[i])+" and ";
				}
				i++;
				//				select  floor(st_x/ 0.28300000000000003) as x,
				//						floor(st_y/ 0.10999999999999943) as y,
				//						timel / 86400 as t,
				//						count(*) as cnt
				//				from london_neogeo
				//				where   st_x>=0 and st_x<=0.28300000000000003 and
				//						st_y>=51.370000000000005 and st_y<=51.59 and
				//						timel>=1318377600 and timel<=1319932800
				//				group by floor(st_x/ 0.28300000000000003), 
				//						 floor(st_y/ 0.10999999999999943), 
				//				 		 timel / 86400;
			}
			if (sql_group.endsWith(",")) 
				sql_group = sql_group.substring(0, sql_group.length()-1);
			String sql = sql_sel+"count(*) from "+pa.table+sql_constr+" true "+sql_group;
			System.out.println(sql);
			Statement stmt = c.createStatement();
			stmt.execute(sql);
			rs=stmt.getResultSet();
			while(rs.next()){
				System.out.println(rs.getDouble(1)+"|"+rs.getLong(2));
			}

			rs.close();


			// GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, "public", "london_hav_neogeo", "myAggregate");
			//
			// pa.boxQuery("count",0.18471,51.60626,0.23073,51.55534); // in the middle of havering map *correction anomaly
			//			pa.boxQuery("count",-0.058,51.59,0.095,51.483); // left of havering, few tweets
			// pa.boxQuery("count",-0.058,51.58961,0.095,51.48287); // left of havering, few tweets
			// pa.boxQuery("count",-0.38326,51.62780,0.14554,51.39572); // a big london query
			// pa.boxQuery("count",-8.4,60,1.9,49); // the entire UK query

			// pa.boxQuery3d("count",-0.058,51.58961,0.095,51.48287,new Timestamp(1319000000000L), new Timestamp(1319900000000L)); // left of havering, few tweets
			// pa.boxQuery3d("count",0.18471,51.60626,0.23073,51.55534,new Timestamp(1319000000000L), new Timestamp(1319900000000L)); // in the middle of havering map *correction anomaly

			double vertcells = 70;
			//		     pa.createAggrGrid("uk_grid","count",(double)(60-49)/vertcells,-8.4,60,1.9,49); // the entire UK query
			//			int[] iv_count = {10, 10};
			//Double[][] iv_first_obj = new Double[2][2];
			// lowX,highY,highX,low = -8.4,60,1.9,49
			// pa.SQLquery_grid(PreAggregate.AGGR_COUNT, iv_first_obj, iv_count);
			//			if ( false ) {
			//				PegelAndelfingen2Aggregate pegel = new PegelAndelfingen2Aggregate(c, "public" , "andelfingen2", "pegel_andelfingen2", "timed");
			//				// PegelAndelfingen2Aggregate pegel = new PegelAndelfingen2Aggregate(c, "public" , "andelfingen2", "pegel_andelfingen2");
			//				pegel.timeQuery("count", 1167606600, 1312737480);
			//			}							 

			c.close();
		} catch (SQLException e) {
			System.out.println("Caught: " + e);
			e.printStackTrace(System.out);
		}
		System.out.println("#!finished");
	}

	public static void runTest(Connection c, String schema) throws Exception {
		try {
			// new TweetConverter(c,"public","london_hav_raw",c,"public","london_hav");
			// new TweetConverter("/Users/flokstra/twitter_sm.db",c,"public","london_hav");
			// new TweetConverter("/Users/flokstra/uk_raw.sql",c,"public","uk");
			//

			GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, schema, "london_hav_neogeo", null, "myAggregate", "coordinates",0 /* axis 2 split*/,200000,null);
                        //GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, schema, "london_hav_neogeo", "myAggregate"); 
                        
                        //GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, schema, "uk_neogeo", null, "myAggregate", "coordinates",0 /* axis 2 split*/,200000,null);
                        //GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, schema, "uk_neogeo", "myAggregate");
                        
			
                        //GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, "public", "nl_all", null, "myAggregate", "coordinates",1 /* axis 2 split*/,200000,null);

			// GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, "public", "london_hav_neogeo", "myAggregate");
			//
			 //pa.boxQuery("count",0.18471,51.60626,0.23073,51.55534); // in the middle of havering map *correction anomaly
			// pa.boxQuery("count",-0.058,51.59,0.095,51.483); // left of havering, few tweets
			// pa.boxQuery("count",-0.058,51.58961,0.095,51.48287); // left of havering, few tweets
			
			// pa.boxQuery("count",-0.38326,51.62780,0.14554,51.39572); // a big london query
			//pa.boxQuery("count",-8.4,60,1.9,49); // the entire UK query

			// pa.boxQuery3d("count",-0.058,51.58961,0.095,51.48287,new Timestamp(1319000000000L), new Timestamp(1319900000000L)); // left of havering, few tweets
			// pa.boxQuery3d("count",0.18471,51.60626,0.23073,51.55534,new Timestamp(1319000000000L), new Timestamp(1319900000000L)); // in the middle of havering map *correction anomaly

			//pa.boxQuery("count",-0.058,51.59,0.095,51.483); // left of havering, few tweets
			//pa.boxQuery("count",4.3, 51.8,4.6,52.1); // ergens bij rotterdam
                        
			System.exit(0);
			
			double vertcells = 70;
			// pa.createAggrGrid("uk_grid","count",(double)(60-49)/vertcells,-8.4,60,1.9,49); // the entire UK query

			//			int[] iv_count = {10, 10};
			//			Double[][] iv_first_obj = new Double[2][2];
			//			iv_first_obj[0][0] = -0.11;
			//			iv_first_obj[0][1] = -0.07;
			//			iv_first_obj[1][0] = 51.33;
			//			iv_first_obj[1][1] = 51.36;

			// out of range example
			int[] iv_count = {20, 20};
			Double[][] iv_first_obj = new Double[2][2];
			iv_first_obj[0][0] = -0.30;
			iv_first_obj[0][1] = -0.26;
			iv_first_obj[1][0] = 51.20;
			iv_first_obj[1][1] = 51.23;

			pa.SQLquery_grid(PreAggregate.AGGR_COUNT, iv_first_obj, iv_count);
			//			if ( false ) {
			//				PegelAndelfingen2Aggregate pegel = new PegelAndelfingen2Aggregate(c, "public" , "andelfingen2", "pegel_andelfingen2", "timed");
			//				// PegelAndelfingen2Aggregate pegel = new PegelAndelfingen2Aggregate(c, "public" , "andelfingen2", "pegel_andelfingen2");
			//				pegel.timeQuery("count", 1167606600, 1312737480);
			//			}							 

			c.close();
		} catch (SQLException e) {
			System.out.println("Caught: " + e);
			e.printStackTrace(System.out);
		}
		System.out.println("#!finished");
	}

	public static void runTest3(Connection c, String schema) throws Exception {
		double	DFLT_BASEBOXSIZE = 0.001;
		short	DFLT_N = 4;
		//GeotaggedTweetAggregate pa = new GeotaggedTweetAggregate(c, "public", "uk_neogeo", "myAggregate", "coordinates",-1,200000,null);
                
                AggregateAxis axis[] = null;
                if (SqlUtils.dbType(c) == DbType.MONETDB) {
                    axis = new AggregateAxis[]{
                            new MetricAxis("coordinates_x", "double", "" + DFLT_BASEBOXSIZE,DFLT_N),
                            new MetricAxis("coordinates_y", "double", "" + DFLT_BASEBOXSIZE,DFLT_N),
                            new MetricAxis("\"time\"", "timestamp with time zone", "360000" /*=10 min*/, (short)16)
                    };
                } else {                
                    axis = new AggregateAxis[]{
                            new MetricAxis("ST_X(coordinates)", "double", "" + DFLT_BASEBOXSIZE,DFLT_N),
                            new MetricAxis("ST_Y(coordinates)", "double", "" + DFLT_BASEBOXSIZE,DFLT_N),
                            new MetricAxis("time", "timestamp with time zone", "360000" /*=10 min*/, (short)16)
                    };
                }
                
		//PreAggregate pa = new PreAggregate(c,"public", "uk_neogeo", null /*override_name*/, "myAggregate",axis,"char_length(tweet)","bigint",PreAggregate.AGGR_ALL,2,200000,null);
                PreAggregate pa = new PreAggregate(c, schema, "london_hav_neogeo", null /*override_name*/, "myAggregate", axis, "len" , "bigint", PreAggregate.AGGR_ALL, 2, 200000, null);

	}
	
	public static void runTest_nominal(Connection c) throws Exception {
		try {
			String wordlist_eng = 
				NominalAxis.ALL + "," +
				"complain,"+
				"alarm,"+
				"smell,"+
				"banker," +
				"car," +
				"stink"
			;
			
			String wordlist = 
				NominalAxis.ALL + "," +
				"klacht,"+
				"hinder,"+
				"stank,"+
				"ruik,"+
				"lucht,"+
				"stinkt,"+
				"meurt,"+
				"smerig,"+
				"geur,"+
				"vies,"+
				"vieze,"+
				"ranzig,"+
				"olie ,"+
				"gist,"+
				"chemisch,"+
				"gas,"+
				"stof,"+
				"roet,"+
				"korrels,"+
				"deeltjes,"+
				"poeder,"+
				"lawaai,"+
				"overlast,"+
				"geluid,"+
				"herrie,"+
				"horeca,"+
				"feest,"+
				"evenement,"+
				"knal,"+
				"dreun,"+
				"ontploffing,"+
				"explosie,"+
				"veiligheid,"+
				"incident,"+
				"gevaarlijk,"+
				"ongeval,"+
				"meting,"+
				"grip,"+
				"brand,"+
				"fakkel,"+
				"vuur,"+
				"vlammen,"+
				"rook,"+
				"pluim,"+
				"fik,"+
				"alarm,"+
				"sirene,"+
				"maass,"+
				"Europoort,"+
				"Botlek,"+
				"Pernis,"+
				"Maasvlakte,"+
				"Waalhaven,"+
				"vlaard,"+
				"briel,"+
				"hoogvl,"+
				"rozenb,"+
				"westvoorne,"+
				"schied,"+
				"prikkel,"+
				"branderig,"+
				"storing,"+
				"stroom"
			;

			NominalGeoTaggedTweetAggregate pa = new NominalGeoTaggedTweetAggregate(c, wordlist, "public", "london_hav_neogeo", null, "myAggregate", "coordinates",-1,200000,null);
			// NominalGeoTaggedTweetAggregate pa = new NominalGeoTaggedTweetAggregate(c, wordlist, "public", "nl_all", null, "myAggregate", "coordinates",1,200000,null);
			// NominalGeoTaggedTweetAggregate pa = new NominalGeoTaggedTweetAggregate(c,"public", "uk_neogeo", "myAggregate");
			// NominalGeoTaggedTweetAggregate pa = new NominalGeoTaggedTweetAggregate(c, wordlist_eng, "public", "uk_neogeo", null, "myAggregate", "coordinates",1,200000,null);

			pa.boxQuery_word("count",-0.058,51.59,0.095,51.483,"alarm"); // left of havering, few tweets
			// pa.test_SQLquery_grid("count",4.3, 51.8,4.6,52.1,"alarm"); // ergens bij rotterdam
			
			System.exit(0);

			c.close();
		} catch (SQLException e) {
			System.out.println("Caught: " + e);
			e.printStackTrace(System.out);
		}
		System.out.println("#!finished");
	}
	
	public static void runTest_small_nominal(Connection c) throws Exception {
		try {
			String wordlist = 
				NominalAxis.ALL + "," +
				"banker,"+
				"car,"+
				"people,"
			;

			
			NominalGeoTaggedTweetAggregate pa = new NominalGeoTaggedTweetAggregate(c, wordlist, "public", "london_hav_neogeo", null, "myAggregate", "coordinates",-1,200000,null);
			// pa.boxQuery_word("count",0.18471,51.60626,0.23073,51.55534,"banker"); // left of havering, few tweets
			pa.boxQuery_word("count",-0.38326,51.62780,1.14554,51.39572,NominalAxis.ALL);
			c.close();
		} catch (SQLException e) {
			System.out.println("Caught: " + e);
			e.printStackTrace(System.out);
		}
		System.out.println("#!finished");
	}	
	
}
