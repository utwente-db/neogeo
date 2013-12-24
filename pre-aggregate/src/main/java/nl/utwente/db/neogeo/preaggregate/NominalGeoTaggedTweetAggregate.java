package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.SQLException;

public class NominalGeoTaggedTweetAggregate extends PreAggregate {

	public static final String NOMINAL_POSTFIX = "_words";
	
	NominalAxis nominal_axis = null;
	
	/* constructor for already existing aggregate */
	public NominalGeoTaggedTweetAggregate(Connection c, String schema, String table, String label) 
	throws SQLException {
		super(c,schema,table+NOMINAL_POSTFIX,label);
		
		nominal_axis = (NominalAxis)axis[2];
	}
	
	/*  constructor which (re)creates the aggregate */
	public NominalGeoTaggedTweetAggregate(Connection c, String wordlist, String schema, String table, String override_name, String label, String point_column, int axisToSplit, long chunkSize, Object[][] newRange)
		throws SQLException {
		String word_column = "tweet";
		AggregateAxis x_axis = new MetricAxis("ST_X("+point_column+")","double",""+GeotaggedTweetAggregate.DFLT_BASEBOXSIZE,GeotaggedTweetAggregate.DFLT_N);
		AggregateAxis y_axis = new MetricAxis("ST_Y("+point_column+")","double",""+GeotaggedTweetAggregate.DFLT_BASEBOXSIZE,GeotaggedTweetAggregate.DFLT_N);
		nominal_axis = new NominalAxis(word_column, word_column+"_wid", wordlist);
		nominal_axis.tagWordIds2Table(c,schema,table,table+NOMINAL_POSTFIX);
		
		AggregateAxis axis[] = {
			x_axis, 
			y_axis,
			nominal_axis
		};
		createPreAggregate(c,schema,table+NOMINAL_POSTFIX,override_name, label,axis,"char_length(tweet)","bigint",AGGR_ALL,axisToSplit,chunkSize,newRange);
	}
	
	public long boxQuery_word(String aggr, double x1, double y1, double x2, double y2, String word2match) throws SQLException {		
		Object ranges[][] = new Object[3][2];
		ranges[0][0] = new Double(Math.min(x1,x2));
		ranges[0][1] = new Double(Math.max(x1,x2));
		ranges[1][0] = new Double(Math.min(y1,y2));
		ranges[1][1] = new Double(Math.max(y1,y2));
		int nv = nominal_axis.getWordIndex(word2match);
		if ( nv < 0 )
				throw new SQLException(nominal_axis.toString() + " cannot find word: "+word2match);
		ranges[2][0] = nv;
		ranges[2][1] = nv+1;
		return query(aggr,ranges);
	}
	
}
