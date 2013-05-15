package nl.utwente.db.neogeo.preaggregate.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import nl.utwente.db.neogeo.preaggregate.AggregateAxis;

public class PegelAndelfingen2Aggregate extends PreAggregate {

	/*
	 * For SRID:4326 1 degree is approx 111 km. So when choosing a boxsize of
	 * approx 100m we should take 0.001 degrees.
	 */
	public static final int  	DFLT_TIME = 60; // corresponds to ten minutes
	public static final short	DFLT_N = 4;
	
	/* constructor for already existing aggregate */
	public PegelAndelfingen2Aggregate(Connection c, String schema, String table, String label) 
	throws SQLException {
		super(c,schema,table,label);
	}
	
	/*  constructor which (re)creates the aggregate */
	public PegelAndelfingen2Aggregate(Connection c, String schema, String table, String label, String time_column)
		throws SQLException {
		AggregateAxis axis[] = {
				new AggregateAxis(time_column,"long",DFLT_TIME ,DFLT_N)
			};
		createPreAggregate(c,schema,table,label,axis,"PEGEL","double");
	}
		
	public long timeQuery(String aggr, long t1, long t2) throws SQLException {		
		Object ranges[][] = new Object[1][2];
		ranges[0][0] = t1;
		ranges[0][1] = t2;
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
