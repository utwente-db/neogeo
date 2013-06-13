package nl.utwente.db.neogeo.preaggregate;

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
		createPreAggregate(c,schema,table,label,axis,"PEGEL","double precision",AGGR_ALL,-1,-1,null);
	}
		
	public long timeQuery(String aggr, long t1, long t2) throws SQLException {		
		Object ranges[][] = new Object[1][2];
		ranges[0][0] = t1;
		ranges[0][1] = t2;
		return query(aggr,ranges);
	}
	
}
