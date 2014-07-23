package org.geotools.data.aggregation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Vector;
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
			"type text,"+
			"resolution_x integer,"+
			"resolution_y integer,"+
			"resolution_time integer,"+
			"response_time double precision,"+
			"\"time\" timestamp with time zone) ";
	public static final String LOG_INSERT_QUERY = "insert into pre_aggregate_logging "+
			"(tablename,label,request,ip,"+
			" aggregate,low_x,high_x,low_y,high_y,"+
			" start_time,end_time, type,"+
			" resolution_x, resulution_y, resolution_time,"+
			"response_time,\"time\") values ";

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


	public AggregationDataStore(DbType dbType, String hostname, int port, String schema, String database, String username, String password, int xSize, int ySize, int timeSize, int mask){
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
                
		con = getConnection();
                
		Statement stmt;
		try {
			LOGGER.severe(LOG_QUERY);
			if(!SqlUtils.existsTable(con, schema, "pre_aggregate_logging")){
				stmt = con.createStatement();
				stmt.execute(LOG_QUERY);
				stmt.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		List<Name> ret = null;
		getConnection();
		try {
			List<String> names = PreAggregate.availablePreAggregates(con,schema);
			ret = new Vector<Name>();
			for(String name : names){
				ret.add(new NameImpl(name));
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

	public PreAggregate createPreAggregate(String typename) throws SQLException{
		String tablename = PreAggregate.getTablenameFromTypeName(typename);
		String label = PreAggregate.getLabelFromTypeName(typename);
		Connection c = getConnection();
		// System.out.println("JF:succes connection: "+c);
		return new PreAggregate(dbType, c,schema,tablename,label);
	}

	public void logQuery(Request req, PreAggregate agg, int mask, Area a, Timestamp start_time, Timestamp end_time, int[] range, String type, double response_time) {
		HttpServletRequest httpReq = req.getHttpRequest();
		String ip = httpReq.getRemoteAddr();
		String request = req.getRequest();

		String sql = LOG_INSERT_QUERY;
		sql += "('"+agg.getTable()+"','"+agg.getLabel()+"','"+request+"','"+ip;
		sql += "',"+a.getLowX()+","+a.getHighX()+","+a.getLowY()+","+a.getHighY();
		if ( start_time == null )
			sql += ",NULL,NULL";
		else
			sql += ","+start_time.getTime()+","+end_time.getTime();
		sql += ",'"+type+"',"+range[0]+","+range[1]+",";
		sql += range.length>2 ? range[2] : null;
		sql += ","+response_time+",current_timestamp);";
		Connection con = getConnection();
		Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.execute(sql);
			stmt.close();
		} catch (SQLException e) {
			LOGGER.severe("logging of the query failed: "+e.getMessage());
		}

	}
}
