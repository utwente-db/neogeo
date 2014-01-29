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
import nl.utwente.db.neogeo.preaggregate.MetricAxis;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.DoubleAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.IntegerAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.LongAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.TimestampAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.NominalAxis;
import nl.utwente.db.neogeo.preaggregate.NominalGeoTaggedTweetAggregate;
import nl.utwente.db.neogeo.preaggregate.SqlUtils;

import org.geotools.data.store.ContentEntry;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class PreAggregate extends
nl.utwente.db.neogeo.preaggregate.PreAggregate {
	// NOMINALCHANGE
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

	HashMap<String, AggregateAxis> map = new HashMap<String, AggregateAxis>();

	private int crsNumber = -1;


	private static String detectNominal(String t) {
		System.out.println("XXXXXXXXX detectNominal="+t);
		if ( t.endsWith(NominalGeoTaggedTweetAggregate.NOMINAL_POSTFIX) ) {
			t = t.replace(NominalGeoTaggedTweetAggregate.NOMINAL_POSTFIX,"");
			System.out.println("XXXXXXXXX replace detectNominal="+t);
		}
		return t;
	}
	
	public PreAggregate(Connection c, String schema, String table, String label) throws SQLException{
		super(c,schema,detectNominal(table),label);
		for(AggregateAxis a : axis){
			if(a.columnExpression().startsWith("ST_X")){
				map.put("x", a);
			} else if(a.columnExpression().startsWith("ST_Y")){
				map.put("y", a);
			} else if(a.sqlType().equals(TimestampAxisIndexer.TYPE_EXPRESSION)){
				map.put("time", a);	
			} else if ( a instanceof NominalAxis )
				map.put("nominal",a);
		}
	}

	public Area getArea() {
		MetricAxis x = getXaxis();
		MetricAxis y = getYaxis();
		return new Area((Double)x.low(),(Double)x.high(),(Double)y.low(),(Double)y.high());
	}

	public long[] getTimeBounds(){
		long[] ret = new long[2];
		MetricAxis a = getTimeAxis();
		ret[0] = ((Timestamp) a.low()).getTime();
		ret[1] = ((Timestamp) a.high()).getTime();
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

	public MetricAxis getXaxis(){
		return (MetricAxis)map.get("x");
	}

	public MetricAxis getYaxis(){
		return (MetricAxis)map.get("y");
	}

	public MetricAxis getTimeAxis(){
		return (MetricAxis)map.get("time");
	}
	
	public NominalAxis getNominalAxis(){
		return (NominalAxis)map.get("nominal");
	}

	public ResultSet StandardSQLQuery_grid(int queryAggregateMask, Object iv_first_obj[][], int iv_count[]) throws SQLException {
		int i=0;
		String sql_sel = "select ";
		String sql_constr = " where ";
		String sql_group = " group by ";
//		System.out.println("cnt | low | factor | high");
		for(AggregateAxis a : getAxis()){
			if(iv_first_obj[i][1] instanceof Double){
				double start = (Double)iv_first_obj[i][0];
				double end = (Double)iv_first_obj[i][1];
//				System.out.print(iv_count[i]+"|"+iv_first_obj[i][0]+"|");
//				System.out.print((end-start)+"|");
//				System.out.println(start+(end-start)*iv_count[i]);
				if(iv_count[i]>1){
					sql_sel += " floor("+a.columnExpression()+"/"+ (end-start)+") as a"+i+",";
					sql_group += " floor("+a.columnExpression()+"/"+ (end-start)+") ,";
				}
				sql_constr += " "+a.columnExpression()+">="+start+" and "+a.columnExpression()+"<="+(start+(end-start)*iv_count[i])+" and ";
			}
			if(iv_first_obj[i][1] instanceof Timestamp){
				long start = ((Timestamp)iv_first_obj[i][0]).getTime()/1000;
				long end = ((Timestamp)iv_first_obj[i][1]).getTime()/1000;
//				System.out.print(iv_count[i]+"|"+start+"|");
//				System.out.print((end-start)+"|");
//				System.out.println(start+(end-start)*iv_count[i]);
				if(iv_count[i]>1){
					sql_sel += " EXTRACT(EPOCH FROM "+a.columnExpression()+")/"+ (end-start)+" as a"+i+",";
					sql_group += " EXTRACT(EPOCH FROM "+a.columnExpression()+")/"+ (end-start)+" ,";
				}
				sql_constr += " EXTRACT(EPOCH FROM "+a.columnExpression()+")>="+start+" and EXTRACT(EPOCH FROM "+a.columnExpression()+")<="+(start+(end-start)*iv_count[i])+" and ";
			}
			i++;
		}
		if (sql_group.endsWith(",")) 
			sql_group = sql_group.substring(0, sql_group.length()-1);
		String sql = sql_sel+"count(*) from "+table+sql_constr+" true "+sql_group;
		Statement stmt = c.createStatement();
		LOGGER.severe(sql);
		ResultSet rs=stmt.getResultSet();
		return rs;
	}

}
