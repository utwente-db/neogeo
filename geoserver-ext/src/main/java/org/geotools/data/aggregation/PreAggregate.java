package org.geotools.data.aggregation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Logger;

import nl.utwente.db.neogeo.preaggregate.AggregateAxis;
import nl.utwente.db.neogeo.preaggregate.SqlUtils;

import org.geotools.data.store.ContentEntry;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import nl.utwente.db.neogeo.preaggregate.AggregateAxis.DoubleAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis.IntegerAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis.LongAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.AggregateAxis.TimestampAxisIndexer;


public class PreAggregate extends
nl.utwente.db.neogeo.preaggregate.PreAggregate {
	private static final Logger LOGGER = Logger.getLogger("org.geotools.data.aggregation.PreAggregate");
	private static final String NAME = "aggregate";
	
	// TODO other type mappings have to be added
	private static final HashMap<String, String> className = new HashMap<String, String>()
	{{put(LongAxisIndexer.TYPE_EXPRESSION,Long.class.getCanonicalName());
	put(IntegerAxisIndexer.TYPE_EXPRESSION,Integer.class.getCanonicalName());
	put(DoubleAxisIndexer.TYPE_EXPRESSION,Double.class.getCanonicalName());
	put(TimestampAxisIndexer.TYPE_EXPRESSION, Timestamp.class.getCanonicalName());}};
	
	private static final String NATIVE_SRS_QUERY = "SELECT ST_SRID(COLUMN) FROM TABLE limit 1";
	// first ? is the string tablename+"_"+label
	private static final String GEOMETRY_COLUMN_QUERY = "SELECT tablename, substr(columnexpression,6,length(columnexpression)-6) FROM pre_aggregate_axis where tablename || '___' ||label=? and substr(columnexpression,1,4)='ST_X'";
//	private static final String BOUNDS_QUERY = "SELECT tablename, substr(columnexpression,1,4), low, high FROM pre_aggregate_axis where tablename || '___' ||label=? and substr(columnexpression,1,3)='ST_' order by columnexpression";
	
	private int crsNumber = -1;
	

	public PreAggregate(Connection c, String schema, String table, String label) throws SQLException{
		super(c,schema,table,label);
	}

	public Area getArea() {
		double x1 = 0, x2 = 0, y1 = 0, y2 = 0;
		for(AggregateAxis a : axis){
			if(a.columnExpression().startsWith("ST_X")){
				x1 = (Double) a.low();
				x2 = (Double) a.high();
			} else if(a.columnExpression().startsWith("ST_Y")){
					y1 = (Double) a.low();
					y2 = (Double) a.high();
				}
		}
		return new Area(x1,x2,y1,y2);
	}

	public long[] getTimeBounds(){
		long[] ret = new long[2];
		for(AggregateAxis a : axis){
			if(a.sqlType().equals(TimestampAxisIndexer.TYPE_EXPRESSION)){
				ret[0] = ((Timestamp) a.low()).getTime();
				ret[1] = ((Timestamp) a.high()).getTime();
				break;
			}
		}
		return ret;
	}
	
	static public List<String> availablePreAggregates(Connection c, String schema) throws SQLException{
		if (!SqlUtils.existsTable(c, schema, PreAggregate.aggregateRepositoryName)) return null;
		String query = "select tablename,label from "+aggregateRepositoryName+";";
		ResultSet rs = SqlUtils.execute(c,query);
		Vector<String> ret = new Vector<String>();
		while(rs.next()){
			ret.add(getTypeName(rs.getString("tablename"),rs.getString("label")));
		}
		return ret;
	}

	public static String getTypeName(String tablename, String label) {
		return NAME+"_"+tablename+"___"+label;
	}

	public static String getTablenameFromTypeName(String typename) {
		if(typename.contains("___"))
			return typename.split("___")[0];
		else return typename;
	}

	public static String getLabelFromTypeName(String typename) {
		if(typename.contains("___"))
			return typename.split("___")[1];
		else return "";
	}

	public Map<String,Class> getColumnTypes(int mask) throws ClassNotFoundException{
		Class cl = Class.forName(className.get(aggregateType));
		LinkedHashMap<String,Class> ret = new LinkedHashMap<String,Class>();
		if((mask & this.aggregateMask & PreAggregate.AGGR_COUNT) != 0)
			ret.put("countaggr", Long.class);
		if((mask & this.aggregateMask & PreAggregate.AGGR_SUM) != 0)
			ret.put("sumaggr", cl);
		if((mask & this.aggregateMask & PreAggregate.AGGR_MIN) != 0)
			ret.put("min", cl);
		if((mask & this.aggregateMask & PreAggregate.AGGR_MAX) != 0)
			ret.put("max", cl);
		// check whether time is contained in the axis
		for(AggregateAxis a : axis){
			if(TimestampAxisIndexer.TYPE_EXPRESSION.equals(a.sqlType())){
				// we have a time axis
				ret.put("starttime", Timestamp.class);
				ret.put("endtime", Timestamp.class);
			} 
		}
		return ret;
	}

	public int getCRSNumber(ContentEntry entry) {
		int ret = 0;
		String typename = entry.getTypeName();
		typename = typename.substring((NAME+"_").length());
		PreparedStatement stmt1;
		Connection con = ((AggregationDataStore)entry.getDataStore()).getConnection();
		try {
			stmt1 = con.prepareStatement(GEOMETRY_COLUMN_QUERY);
			stmt1.setString(1, typename);
			LOGGER.finest("geometry column query:"+stmt1.toString());
			stmt1.execute();
			ResultSet rs1 = stmt1.getResultSet();
			String tablename = "";		
			String locColumn = "";
			while(rs1.next()){
				tablename = rs1.getString(1);
				locColumn = rs1.getString(2);
			}
			rs1.close();
			Statement stmt2;
			try {
				String query = NATIVE_SRS_QUERY.replaceFirst("COLUMN", locColumn).
				replaceFirst("TABLE", tablename);
				stmt2 = con.createStatement();			
				LOGGER.finest("NATIVE SRS query:"+stmt2.toString());
				stmt2.execute(query);
				ResultSet rs2 = stmt2.getResultSet();
				while(rs2.next()){
					ret = rs2.getInt(1);
				}
				rs2.close();
			} catch (SQLException e) {

				e.printStackTrace();
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		return ret;
	}

	public static String stripTypeName(String typename) {
		return typename.substring((NAME+"_").length());
	}

	public CoordinateReferenceSystem getCoordinateReferenceSystem(ContentEntry entry){
		CRSAuthorityFactory   factory = CRS.getAuthorityFactory(true);
		CoordinateReferenceSystem crs = DefaultGeographicCRS.WGS84; // <- Coordinate reference system
		try {
			if(crsNumber==-1)
				crsNumber = getCRSNumber(entry);
			crs = factory.createCoordinateReferenceSystem("EPSG:"+crsNumber);
		} catch (NoSuchAuthorityCodeException e) {
			e.printStackTrace();
		} catch (FactoryException e) {
			e.printStackTrace();
		}
		return crs;
	}

}
