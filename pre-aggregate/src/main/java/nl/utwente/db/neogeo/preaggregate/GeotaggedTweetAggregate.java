package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

public class GeotaggedTweetAggregate extends PreAggregate {

	/*
	 * For SRID:4326 1 degree is approx 111 km. So when choosing a boxsize of
	 * approx 100m we should take 0.001 degrees.
	 */
	public static final double	DFLT_BASEBOXSIZE = 0.001;
	public static final short	DFLT_N = 4;
	
	/* constructor for already existing aggregate */
	public GeotaggedTweetAggregate(Connection c, String schema, String table, String label) 
	throws SQLException {
		super(c,schema,table,label);
	}
	
	/*  constructor which (re)creates the aggregate */
	public GeotaggedTweetAggregate(Connection c, String schema, String table, String label, String point_column)
		throws SQLException {
		AggregateAxis axis[] = {
				new AggregateAxis("ST_X("+point_column+")","double",""+DFLT_BASEBOXSIZE,DFLT_N),
				new AggregateAxis("ST_Y("+point_column+")","double",""+DFLT_BASEBOXSIZE,DFLT_N)
				// , new AggregateAxis("time","timestamp with time zone","3600000" /*=1 hour*/,DFLT_N)
			};
		createPreAggregate(c,schema,table,label,axis,"char_length(tweet)","bigint",AGGR_ALL);
	}
	
	public long boxQuery(String aggr, double x1, double y1, double x2, double y2) throws SQLException {		
		Object ranges[][] = new Object[2][2];
		ranges[0][0] = new Double(Math.min(x1,x2));
		ranges[0][1] = new Double(Math.max(x1,x2));
		ranges[1][0] = new Double(Math.min(y1,y2));
		ranges[1][1] = new Double(Math.max(y1,y2));
		long res = query(aggr,ranges);
		if ( false ) SQLquery((AGGR_COUNT|AGGR_MIN),ranges);
		if ( false ) {
			int count[] = new int[axis.length];
			for(int i=0; i<axis.length; i++)
				count[i] = 2;
			SQLquery_grid((AGGR_COUNT|AGGR_MIN),ranges,count);
		}
		return res;
	}
	
	public long boxQuery_multi(String aggr, double x1, double y1, double x2, double y2) throws SQLException {		
		Object multi_ranges[][][] = new Object[2][2][2];
		Object ranges[][];
		ranges = new Object[2][2];
		ranges[0][0] = new Double(Math.min(x1,x2));
		ranges[0][1] = new Double(Math.max(x1,x2));
		ranges[1][0] = new Double(Math.min(y1,y2));
		ranges[1][1] = new Double(Math.max(y1,y2));
		multi_ranges[0] = ranges;
		
		ranges = new Object[2][2];
		ranges[0][0] = new Double(Math.min(x1,x2)+0.5);
		ranges[0][1] = new Double(Math.max(x1,x2)+0.5);
		ranges[1][0] = new Double(Math.min(y1,y2)+0.5);
		ranges[1][1] = new Double(Math.max(y1,y2)+0.5);
		multi_ranges[1] = ranges;
		
		SQLquery_interval((AGGR_COUNT|AGGR_MIN),multi_ranges);
		return 0;
	}
	
	public long boxQuery3d(String aggr, double x1, double y1, double x2, double y2, Timestamp z1, Timestamp z2) throws SQLException {		
		Object ranges[][] = new Object[3][2];
		ranges[0][0] = new Double(Math.min(x1,x2));
		ranges[0][1] = new Double(Math.max(x1,x2));
		ranges[1][0] = new Double(Math.min(y1,y2));
		ranges[1][1] = new Double(Math.max(y1,y2));
		ranges[2][0] = z1;
		ranges[2][1] = z2;
		return query(aggr,ranges);
	}
	
/**	public void createAggrGrid(String newtable, String aggr, double cellSize, double x1, double y1, double x2, double y2) throws SQLException {	
		// first, the cellSize should be a multiple of BASEBOXSIZE
		double BASEBLOCKSIZE = ((Double)axis[0].BASEBLOCKSIZE()).doubleValue();
		int cellElements = (int)(cellSize/BASEBLOCKSIZE);
		cellSize = cellElements*BASEBLOCKSIZE;
		
		double xmin = Math.min(x1,x2);
		double ymin = Math.min(y1,y2);
		int lxmin = axis[0].getIndex(new Double(xmin));
		xmin = ((Double)axis[0].reverseValue(lxmin)).doubleValue();
		long xcount= (long)Math.ceil((Math.max(x1,x2)-xmin)/cellSize);
		int lymin = axis[1].getIndex(new Double(ymin));
		ymin = ((Double)axis[1].reverseValue(lymin)).doubleValue();
		long ycount= (long)Math.ceil((Math.max(y1,y2)-ymin)/cellSize);
		System.out.println("+ createAggr/cellSize="+cellSize+", xmin="+xmin+", xcount="+xcount+", ymin="+ymin+", ycount="+ycount);
		String subindex = "1";
		String gridkeys = "grid_pacells2d("+lxmin+","+cellElements+","+xcount+","+lymin+","+cellElements+","+ycount+","+axis[0].N()+","+subindex+")";
		String sql = "SELECT gkey,sum(cnt),sum(lsum) FROM "+schema+"."+table+PA_EXTENSION+", "+gridkeys+ " WHERE key=pakey GROUP BY gkey;";
		// SELECT gkey,sum(cnt) FROM public.uk_neogeo_pa, grid_pacells2d(581,220,47,0,220,50,4) WHERE key=pakey GROUP BY gkey;
		// Time: 1138.490 ms
		if ( SqlUtils.existsTable(c, schema, newtable)) {
			SqlUtils.execute(c,
					"SELECT DropGeometryColumn('"+schema+"','"+newtable+"','box');"); 
			SqlUtils.dropTable(c, schema, newtable);
		}
		SqlUtils.executeNORES(c,
				"CREATE TABLE " + schema + "." + newtable + " (" +
						"x int," +
						"y int,"+
						"count bigint," +
						"sum bigint" +
				");"
		);
		SqlUtils.execute(c,
				"SELECT AddGeometryColumn('"+schema+"','"+newtable+"','box','"+"4326"+"','POLYGON',2);"
		);
		
		PreparedStatement ps = c.prepareStatement("INSERT INTO " + schema + "."
				+ newtable + "  (x,y,count,sum,box) VALUES(?,?,?,?,ST_Polygon(ST_GeomFromText(?),"+"4326"+"));");
		
		System.out.println("+ Computing grid: ");
		System.out.println("  sql: "+sql);
		long start_ms = new Date().getTime();
		ResultSet rs = SqlUtils.execute(c, sql);
		long elapsed_ms = new Date().getTime() - start_ms;
		System.out.println("  execution time = "+elapsed_ms+"ms");
		c.setAutoCommit(false);  
		int ncells = 0;
		while (rs.next()) {
			long gkey  = rs.getLong(1);
			long count = rs.getLong(2);
			long sum   = rs.getLong(3);
			int x = (int)(gkey%ycount);
			int y = (int)(gkey/ycount);
			int rbx = lxmin + x * cellElements; 
			double drbx = ((Double)axis[0].reverseValue(rbx)).doubleValue();
			int rby = lymin + y * cellElements; 
			double drby = ((Double)axis[1].reverseValue(rby)).doubleValue(); 

			ncells++;
			// String box = SqlUtils.bbox_linestr(xmin+x*cellSize, ymin+y*cellSize, xmin+(x+1)*cellSize, ymin+(y+1)*cellSize);
			String box = SqlUtils.bbox_linestr(drbx,drby, drbx+(cellElements*BASEBLOCKSIZE), drby+(cellElements*BASEBLOCKSIZE));
			// System.out.println("+ READ(x="+x+", y="+y+", count="+count+", sum="+sum+", box="+box);
			ps.setInt(1, x);
			ps.setInt(2, y);
			ps.setLong(3, count);
			ps.setLong(4, sum);
			ps.setString(5, box);
			
			ps.execute();
		}
		c.commit();
		c.setAutoCommit(true);  
		System.out.println("+ grid table ["+xcount +"x" + ycount +"] with " + ncells +" cells written!");
	}
	**/
}
