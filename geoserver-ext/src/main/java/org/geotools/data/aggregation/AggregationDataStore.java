package org.geotools.data.aggregation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

import nl.utwente.db.neogeo.preaggregate.SqlUtils;
import nl.utwente.db.neogeo.preaggregate.SqlUtils.DbType;

import org.geoserver.ows.Request;
import org.geotools.data.Query;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;
import org.opengis.feature.type.Name;

// Referenced classes of package org.geotools.data.aggregation:
//            AggregationFeatureSource

public class AggregationDataStore extends ContentDataStore {
	private static final Logger LOGGER = Logger.getLogger("org.geotools.data.aggregation.AggregationDataStore");
	public static final String LOG_QUERY = "CREATE TABLE pre_aggregate_logging ( id serial, "+
			"tablename text,"+
			"label text,"+
			"request text,"+
			"ip text,"+ 
			"\"aggregate\" integer,"+ 
			"low_x double precision,"+ 
			"high_x double precision,"+ 
			"low_y double precision,"+ 
			"high_y double precision,"+ 
			"start_time bigint,"+ 
			"end_time bigint,"+
                        "keywords text," +
			"type text,"+
			"resolution_x integer,"+
			"resolution_y integer,"+
			"resolution_time integer,"+
			"response_time double precision,"+
			"\"time\" timestamp with time zone) ";
	
        protected PreparedStatement logQuery;

        private DbType dbType;
	private String hostname;
	private int port;
	private String username;
	private String password;
	private String database;
	private Connection con;
	private String schema;
	private int xSize;
	private int ySize;
	private int timeSize;
	private int mask;
        private boolean enableServersideStairwalk;
        private boolean enableLogging;


	public AggregationDataStore(DbType dbType, String hostname, int port, String schema, String database, 
                String username, String password, int xSize, int ySize, int timeSize, int mask,
                boolean enableServersideStairwalk, boolean enableLogging){
            
                this.dbType = dbType;
		this.hostname = hostname; 
		this.port = port;
		this.username = username;
		this.password = password;
		this.schema = schema;
		this.database = database;
		this.xSize = xSize;
		this.ySize = ySize;
		this.timeSize = timeSize;
		this.mask = mask;
                this.enableServersideStairwalk = enableServersideStairwalk;
                this.enableLogging = enableLogging;
                
		con = getConnection();
                
                if (enableLogging) {
                    Statement stmt;
                    try {
                            LOGGER.severe(LOG_QUERY);
                            if(!SqlUtils.existsTable(con, schema, "pre_aggregate_logging")){
                                    stmt = con.createStatement();
                                    stmt.execute(LOG_QUERY);
                                    stmt.close();
                            }
                            
                            logQuery = con.prepareStatement(
                                "INSERT INTO pre_aggregate_logging (" +
                                SqlUtils.quoteIdentifier(dbType, "tablename") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "label") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "request") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "ip") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "aggregate") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "low_x") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "high_x") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "low_y") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "high_y") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "start_time") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "end_time") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "keywords") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "type") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "resolution_x") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "resolution_y") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "resolution_time") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "response_time") + ", " +
                                SqlUtils.quoteIdentifier(dbType, "time") +
                                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");                            
                    } catch (SQLException e) {
                        LOGGER.severe("Unable to setup query logging!");
                        e.printStackTrace();
                        this.enableLogging = false;
                    }
                }
                 
		LOGGER.severe("AggregationDataStore created!!! ###########");
	}

	/**
	 * return a connection object to a postgres database with aggregation index
	 * @return
	 */
	public Connection getConnection(){
		if(con==null)
			con = _getConnection();
		return con;
	}

	private Connection _getConnection(){
                String className = "";
                if (dbType == DbType.POSTGRES) {
                    className = "org.postgresql.Driver";
                } else if (dbType == DbType.MONETDB) {
                    className = "nl.cwi.monetdb.jdbc.MonetDriver";
                }
            
		try{
			Class.forName(className);
		} catch(ClassNotFoundException e)
		{
			LOGGER.severe("Where is your JDBC Driver (" + className + ")? Include in your library path!");
			e.printStackTrace();
			return null;
		}
		LOGGER.fine("JDBC Driver Registered!");
		Connection connection = null;
		try {
                    StringBuilder connUrl = new StringBuilder();
                    if (dbType == DbType.POSTGRES) {
                        connUrl.append("jdbc:postgresql://");
                    } else if (dbType == DbType.MONETDB) {
                        connUrl.append("jdbc:monetdb://");
                    }

                    connUrl.append(hostname).append(":").append(port).append("/").append(database); 
                    
                    connection = DriverManager.getConnection(connUrl.toString(), username, password);
		}
		catch(SQLException e)
		{
                    LOGGER.severe("Connection Failed! Check output console");
                    e.printStackTrace();
                    return null;
		}
		if(connection == null)
			LOGGER.severe("Failed to make connection!");
		return connection;
	}

	@Override
	protected List<Name> createTypeNames() throws IOException	{
                LOGGER.severe("=== RETRIEVING TYPE NAMES ===");
		List<Name> ret = null;
		getConnection();
		try {
			List<String> names = PreAggregate.availablePreAggregates(con,schema);
			ret = new Vector<Name>();
			for(String name : names){
				ret.add(new NameImpl(this.namespaceURI, name));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	protected ContentFeatureSource createFeatureSource(ContentEntry entry) {
		return new AggregationFeatureSource(entry, Query.ALL);
	}


	public boolean hasOutputCount(){
		return hasOutputCount(PreAggregate.AGGR_ALL);
	}

	public boolean hasOutputCount(int mask2){
		return (mask & mask2 & PreAggregate.AGGR_COUNT)==PreAggregate.AGGR_COUNT;
	}

	public boolean hasOutputSum(){
		return hasOutputSum(PreAggregate.AGGR_ALL);
	}

	public boolean hasOutputSum(int mask2){
		return (mask & mask2 & PreAggregate.AGGR_SUM)==PreAggregate.AGGR_SUM;
	}

	public boolean hasOutputMin(){
		return hasOutputMin(PreAggregate.AGGR_ALL);
	}

	public boolean hasOutputMin(int mask2){
		return (mask & mask2 & PreAggregate.AGGR_MIN)==PreAggregate.AGGR_MIN;
	}

	public boolean hasOutputMax(){
		return hasOutputMax(PreAggregate.AGGR_ALL);
	}

	public boolean hasOutputMax(int mask2){
		return (mask & mask2 & PreAggregate.AGGR_MAX)==PreAggregate.AGGR_MAX;
	}
        
        public DbType getDbType () {
            return this.dbType;
        }

	public int getTotalCount() {
		return xSize*ySize;
	}


	public int getXSize(){
		return xSize;
	}

	public int getYSize(){
		return ySize;
	}

	public int getTimeSize(){
		return timeSize;
	}

	public int getMask(){
		return mask;
	}
        
        public boolean isLoggingEnabled () {
            return this.enableLogging;
        }
        
        public boolean isServersideStairwalkEnabled () {
            return this.enableServersideStairwalk;
        }

	public PreAggregate createPreAggregate(String typename) throws SQLException{
		String tablename = PreAggregate.getTablenameFromTypeName(typename);
		String label = PreAggregate.getLabelFromTypeName(typename);
		Connection c = getConnection();
		// System.out.println("JF:succes connection: "+c);
		PreAggregate pa = new PreAggregate(dbType, c,schema,tablename,label);
                
                pa.enableServersideStairwalk(this.enableServersideStairwalk);
                
                return pa;
	}

        public void logQuery (Request req, PreAggregate agg, int mask, Area a, Timestamp start_time, Timestamp end_time, Vector<String> keywordsList,
                int[] range, String type, double response_time) {
            try {
                this._logQuery(req, agg, mask, a, start_time, end_time, keywordsList, range, type, response_time);
            } catch (SQLException ex) {
                LOGGER.severe("Unable to log query: " + ex.getMessage());
            }
        }
        
	public void _logQuery(Request req, PreAggregate agg, int mask, Area a, Timestamp start_time, Timestamp end_time, Vector<String> keywordsList,
                int[] range, String type, double response_time) throws SQLException {
                // only do logging if it is enabled
                if (this.isLoggingEnabled() == false) return;
            
		HttpServletRequest httpReq = req.getHttpRequest();
		String ip = httpReq.getRemoteAddr();
		String request = req.getRequest();

                logQuery.setString(1, agg.getTable());
                logQuery.setString(2, agg.getLabel());
                logQuery.setString(3, request);
                logQuery.setString(4, ip);
                
                logQuery.setInt(5, mask);
                
                logQuery.setDouble(6, a.getLowX());
                logQuery.setDouble(7, a.getHighX());
                logQuery.setDouble(8, a.getLowY());
                logQuery.setDouble(9, a.getHighY());
                
                if (start_time == null) {
                    logQuery.setNull(10, java.sql.Types.BIGINT);
                    logQuery.setNull(11, java.sql.Types.BIGINT);
                } else {
                    logQuery.setLong(10, start_time.getTime());
                    logQuery.setLong(11, end_time.getTime());
                }
                
                if (keywordsList == null || keywordsList.size() == 0) {
                    logQuery.setNull(12, java.sql.Types.CLOB);
                } else {
                    StringBuilder keywords = new StringBuilder();
                    for(String keyword : keywordsList) {
                        keywords.append(keyword);
                        keywords.append(",");
                    }
                    keywords.deleteCharAt(keywords.length()-1); // remove final comma
                    
                    logQuery.setString(12, keywords.toString());
                }
                
                logQuery.setString(13, type);
                
                logQuery.setInt(14, range[0]);
                logQuery.setInt(15, range[1]);
                
                if (range.length > 2) {
                    logQuery.setInt(16, range[2]);
                } else {
                    logQuery.setNull(16, java.sql.Types.INTEGER);
                }
                
                logQuery.setDouble(17, response_time);
                logQuery.setTimestamp(18, new Timestamp(System.currentTimeMillis()));
                
                logQuery.execute();
                logQuery.clearParameters();
	}
}
