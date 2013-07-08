package org.geotools.data.aggregation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

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


	public AggregationDataStore(String hostname, int port, String schema, String database, String username, String password, int xSize, int ySize, int timeSize, int mask){
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
		try{
			Class.forName("org.postgresql.Driver");
		} catch(ClassNotFoundException e)
		{
			LOGGER.severe("Where is your PostgreSQL JDBC Driver? Include in your library path!");
			e.printStackTrace();
			return null;
		}
		LOGGER.fine("PostgreSQL JDBC Driver Registered!");
		Connection connection = null;
		try
		{
			connection = DriverManager.getConnection((new StringBuilder()).append("jdbc:postgresql://").append(hostname).append(":").append(port).append("/").append(database).toString(), username, password);
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
		return new PreAggregate(getConnection(),schema,tablename,label);
	}
}
