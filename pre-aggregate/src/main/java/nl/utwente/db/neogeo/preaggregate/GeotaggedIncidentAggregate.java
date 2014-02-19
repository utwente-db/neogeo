package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;

import nl.utwente.db.neogeo.preaggregate.MetricAxis.DoubleAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.IntegerAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.LongAxisIndexer;
import nl.utwente.db.neogeo.preaggregate.MetricAxis.TimestampAxisIndexer;

public class GeotaggedIncidentAggregate extends PreAggregate {

	/*
	 * For SRID:4326 1 degree is approx 111 km. So when choosing a boxsize of
	 * approx 100m we should take 0.001 degrees.
	 */
	public static final double	DFLT_BASEBOXSIZE = 0.001;
	public static final long	DFLT_TIME_BASEBOXSIZE = 60*60*24; // per day
	public static final short	DFLT_N = 4;
	public static final String NOMINAL_POSTFIX = "_words";
	static final String wordlist = 
			NominalAxis.ALL + ",1,2,3,4,5,8,9,x";
	
	/* constructor for already existing aggregate */
	public GeotaggedIncidentAggregate(Connection c, String schema, String table, String label) 
	throws SQLException {
		super(c,schema,table,label);
	}
	
	/*  constructor which (re)creates the aggregate */
	public GeotaggedIncidentAggregate(Connection c, String schema, String table, String override_name, String label, String point_column, int axisToSplit, long chunkSize, Object[][] newRange)
		throws SQLException {
		AggregateAxis x_axis = new MetricAxis("ST_X("+point_column+")",DoubleAxisIndexer.TYPE_EXPRESSION,""+DFLT_BASEBOXSIZE,DFLT_N);
		AggregateAxis y_axis = new MetricAxis("ST_Y("+point_column+")",DoubleAxisIndexer.TYPE_EXPRESSION,""+DFLT_BASEBOXSIZE,DFLT_N);
		AggregateAxis time_axis = new MetricAxis("dt_melding",TimestampAxisIndexer.TYPE_EXPRESSION,""+DFLT_TIME_BASEBOXSIZE,DFLT_N);
		//NominalAxis urgency_axis = new NominalAxis("urgentie_code","urgentie_code_wid",wordlist);
		//urgency_axis.tagWordIds2Table(c,schema,table,table+NOMINAL_POSTFIX);
		
		AggregateAxis axis[] = {
			x_axis, 
			y_axis,
			time_axis
			//urgency_axis
		};
		createPreAggregate(c,schema,table,override_name, label,axis,"(EXTRACT(epoch FROM dt_functie_hersteld-dt_melding)/(60*60*24))::integer",IntegerAxisIndexer.TYPE_EXPRESSION,AGGR_ALL,axisToSplit,chunkSize,newRange);
//		createPreAggregate(c,schema,table+NOMINAL_POSTFIX,override_name, label,axis,"(EXTRACT(epoch FROM dt_functie_hersteld-dt_melding)/(60*60*24))::integer",IntegerAxisIndexer.TYPE_EXPRESSION,AGGR_ALL,axisToSplit,chunkSize,newRange);
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
	
	public long boxQuery_nom(String aggr, double x1, double y1, double x2, double y2, int nv) throws SQLException {		
		Object ranges[][] = new Object[3][2];
		ranges[0][0] = new Double(Math.min(x1,x2));
		ranges[0][1] = new Double(Math.max(x1,x2));
		ranges[1][0] = new Double(Math.min(y1,y2));
		ranges[1][1] = new Double(Math.max(y1,y2));
		ranges[2][0] = nv;
		ranges[2][1] = nv+1;
		return query(aggr,ranges);
	}
	
}
