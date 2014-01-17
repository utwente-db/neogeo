package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

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

	public long test_SQLquery_grid(String aggr, double x1, double y1, double x2, double y2, String word2match) throws SQLException {		
		Object ranges[][] = new Object[2][2];
		ranges[0][0] = new Double(Math.min(x1,x2));
		ranges[0][1] = new Double(Math.max(x1,x2));
		ranges[1][0] = new Double(Math.min(y1,y2));
		ranges[1][1] = new Double(Math.max(y1,y2));
		
		int iv_count[] = new int[2];
		iv_count[0] = 1;
		iv_count[1] = 1;
		
		Vector<String> wv = null;
		if ( word2match != null ) {
			wv = new Vector<String>();
			wv.add(word2match);
		}
		
		ResultSet rs = delete_SQLquery_grid(AGGR_COUNT,ranges,iv_count,wv);
		
		return 99;
	}
	
	public ResultSet delete_SQLquery_grid(int queryAggregateMask, Object iv_first_obj[][], int iv_count[], Vector<String> swv) throws SQLException {
		String selectWord = NominalAxis.ALL;
		
		if ( swv != null && (swv.size() > 0) ) {
			if ( swv.size() > 1 )
				System.out.println("INCOMPLETE: only able to select 1 word at this moment");
			selectWord = swv.get(0);
		}
		Object new_ranges[][] = new Object[3][2];
		new_ranges[0][0] = iv_first_obj[0][0];
		new_ranges[0][1] = iv_first_obj[0][1];
		new_ranges[1][0] = iv_first_obj[1][0];
		new_ranges[1][1] = iv_first_obj[1][1];
		new_ranges[2][0] = selectWord;
		new_ranges[2][1] = selectWord;	
		
		int new_iv_count[] = new int[3];
		new_iv_count[0] = iv_count[0];
		new_iv_count[1] = iv_count[1];
		new_iv_count[2] = 1;

		return super.SQLquery_grid(queryAggregateMask,new_ranges,new_iv_count);
	}

}
