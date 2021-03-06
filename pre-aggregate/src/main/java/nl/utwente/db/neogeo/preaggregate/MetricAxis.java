package nl.utwente.db.neogeo.preaggregate;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.logging.Logger;
import nl.utwente.db.neogeo.preaggregate.SqlUtils.DbType;

public class MetricAxis extends AggregateAxis {
	
	public static final int INDEX_TOO_SMALL = -2;
	public static final int INDEX_TOO_LARGE = -1;
	
	protected static final Logger LOGGER = Logger.getLogger("nl.utwente.db.neogeo.preaggregate.AggregateAxis");
	
	interface AxisIndexer {
		public Object 	low();
		public Object 	high();
		public Object 	BASEBLOCKSIZE();
		public int		axisSize();
		public int	   	getIndex(Object value, boolean checkBounds);
		public boolean	exactIndex(Object value);
		public Object 	reverseValue(int index);
		public String   storageFormat(Object o);
		public String	sqlType();
		public String   sqlRangeFunction(Connection c, String name) throws SQLException ;
		public AxisSplitDimension splitAxis(Object low, Object high, int cnt);
	}
	
        public class IntegerAxisIndexer implements AxisIndexer {
		private int low;
		private int high;
		private int axisSize;
		private int BASEBLOCKSIZE;
		public final static String TYPE_EXPRESSION = "integer";
		
		
		public IntegerAxisIndexer(Object low, Object high, Object BASEBLOCKSIZE) {
			this.low = (low instanceof Integer) ? ((Integer)low).intValue() : Integer.parseInt(low.toString());
			this.high = (high instanceof Integer) ? ((Integer)high).intValue() : Integer.parseInt(high.toString());
			this.BASEBLOCKSIZE = (BASEBLOCKSIZE instanceof Integer) ? ((Integer)BASEBLOCKSIZE).intValue() : Integer.parseInt(BASEBLOCKSIZE.toString());
			// 
			this.low  = (int)Math.floor(this.low /this.BASEBLOCKSIZE) * this.BASEBLOCKSIZE;
			this.high = (int)Math.ceil(this.high/this.BASEBLOCKSIZE) * this.BASEBLOCKSIZE;
			if ( !(this.high > this.low) )
				throw new RuntimeException("low > high");
			this.axisSize = (int)(Math.ceil((this.high-this.low)/this.BASEBLOCKSIZE));
		}
		
		public Object low() {
			return new Integer(low);
		}
		
		public Object high() {
			return new Integer(high);
		}
		
		public Object BASEBLOCKSIZE() {
			return new Integer(BASEBLOCKSIZE);
		}
		
		public int axisSize() {
			return this.axisSize;
		}
	
		public int getIndex(Object value, boolean checkBounds) {
			int lvalue;

			if (value instanceof Integer)
				lvalue = ((Integer) value).intValue();
			else
				lvalue = Integer.parseInt("" + value);
			if (checkBounds) {
				if (lvalue < low)
					return INDEX_TOO_SMALL;
				else if (lvalue > high)
					return INDEX_TOO_LARGE;
			}
			return (int) ((lvalue - this.low) / this.BASEBLOCKSIZE);
		}
		
		public boolean exactIndex(Object value) {
			int lvalue;
			
			if ( value instanceof Integer )
				lvalue = ((Integer)value).intValue();
			else
				lvalue = Integer.parseInt(""+value);
			if ( lvalue < low )
				return true;
			else if ( lvalue > high )
				return true;
			return (lvalue % this.BASEBLOCKSIZE) == 0;
		}
		
		public Object reverseValue(int index) {
			return new Integer(this.low + index * this.BASEBLOCKSIZE);
		}
		
		public String storageFormat(Object o) {
			return o.toString();
		}
		
		public String sqlType() {
			return TYPE_EXPRESSION ;
		}
		
		public String sqlRangeFunction(Connection c, String fun) throws SQLException {
			return SqlUtils.gen_Create_Or_Replace_Function(
							c, fun, "v "+sqlType(), "integer",
							"", "\tRETURN FLOOR((v - " + this.low + ") / " + this.BASEBLOCKSIZE + ")" + ";\n"

					);	
		}

		public AxisSplitDimension splitAxis(Object startObj, Object endObj, int cnt) {
                    if (cnt <= 0 || startObj == null || endObj == null) throw new RuntimeException("count, low or high values are invalid");
                                        
                    // convert Strings to Integers
                    if (startObj instanceof String) startObj = Integer.valueOf((String)startObj);
                    if (endObj instanceof String) endObj = Integer.valueOf((String)endObj);
                    
                    // ensure we have Integer objects
                    if ((startObj instanceof Integer) == false) throw new RuntimeException("low value is invalid; not an Integer object");
                    if ((endObj instanceof Integer) == false) throw new RuntimeException("high value is invalid; not an Integer object");
                                     
                    // convert to indexes
                    int startIdx = getIndex(startObj, true);
                    int endIdx = getIndex(endObj, true);
                    
                    // check index bounds
                    if (startIdx == INDEX_TOO_SMALL) throw new RuntimeException("low value is out-of-bounds; too small!");
                    if (startIdx == INDEX_TOO_LARGE) throw new RuntimeException("low value is out-of-bounds; too large!");
                    if (endIdx == INDEX_TOO_SMALL) throw new RuntimeException("high value is out-of-bounds; too small!");
                    if (endIdx == INDEX_TOO_LARGE) throw new RuntimeException("high value is out-of-bounds; too large!");
                    
                    if (cnt == 1) {
                        return new AxisSplitDimension(startObj, endObj, 1);
                    } else {                    
                        int deltaIdx = (int)Math.ceil((endIdx-startIdx) / cnt);                    
                        return new AxisSplitDimension(reverseValue(startIdx), reverseValue(startIdx+deltaIdx), cnt);
                    }
		}
		
	}
	
public class  LongAxisIndexer implements AxisIndexer {
		
		private long low;
		private long high;
		private int    axisSize;
		private long BASEBLOCKSIZE;
		public final static String TYPE_EXPRESSION = "bigint";
		
		
		public LongAxisIndexer(Object low, Object high, Object BASEBLOCKSIZE) {
			this.low = (low instanceof Long) ? ((Long)low).longValue() : Long.parseLong(low.toString());
			this.high = (high instanceof Long) ? ((Long)high).longValue() : Long.parseLong(high.toString());
			this.BASEBLOCKSIZE = (BASEBLOCKSIZE instanceof Long) ? ((Long)BASEBLOCKSIZE).longValue() : Long.parseLong(BASEBLOCKSIZE.toString());
			// 
			this.low  = (long)Math.floor(this.low /this.BASEBLOCKSIZE) * this.BASEBLOCKSIZE;
			this.high = (long)Math.ceil(this.high/this.BASEBLOCKSIZE) * this.BASEBLOCKSIZE;
			if ( !(this.high > this.low) )
				throw new RuntimeException("low > high");
			this.axisSize = (int)(Math.ceil((this.high-this.low)/this.BASEBLOCKSIZE));
		}
		
		public Object low() {
			return new Long(low);
		}
		
		public Object high() {
			return new Long(high);
		}
		
		public Object BASEBLOCKSIZE() {
			return new Long(BASEBLOCKSIZE);
		}
		
		public int axisSize() {
			return this.axisSize;
		}
	
		public int getIndex(Object value, boolean checkBounds) {
			long lvalue;

			if (value instanceof Long)
				lvalue = ((Long) value).longValue();
			else
				lvalue = Long.parseLong("" + value);
			if (checkBounds) {
				if (lvalue < low)
					return INDEX_TOO_SMALL;
				else if (lvalue > high)
					return INDEX_TOO_LARGE;
			}
			return (int) ((lvalue - this.low) / this.BASEBLOCKSIZE);
		}
		
		public boolean exactIndex(Object value) {
			long lvalue;
			
			if ( value instanceof Long )
				lvalue = ((Long)value).longValue();
			else
				lvalue = Long.parseLong(""+value);
			if ( lvalue < low )
				return true;
			else if ( lvalue > high )
				return true;
			return (lvalue % this.BASEBLOCKSIZE) == 0;
		}
		
		public Object reverseValue(int index) {
			return new Long(this.low + index * this.BASEBLOCKSIZE);
		}
		
		public String storageFormat(Object o) {
			return o.toString();
		}
		
		public String sqlType() {
			return TYPE_EXPRESSION;
		}
		
		public String sqlRangeFunction(Connection c, String fun) throws SQLException {
			return SqlUtils.gen_Create_Or_Replace_Function(
							c, fun, "v "+sqlType(), "integer",
							"", "\tRETURN " + SqlUtils.gen_DIV(c,"v - " + this.low, ""+this.BASEBLOCKSIZE) + ";"
					);	
		}

		public AxisSplitDimension splitAxis(Object low, Object high, int cnt) {
			throw new RuntimeException("Function not implemented yet!");
		}
		
		
	}
	
public class DoubleAxisIndexer implements AxisIndexer {
		private double low;
		private double high;
		private int    axisSize;
		private double BASEBLOCKSIZE;
		public final static String TYPE_EXPRESSION = "double precision";
		
		
		public DoubleAxisIndexer(Object low, Object high, Object BASEBLOCKSIZE) {
			this.low = (low instanceof Double) ? ((Double)low).doubleValue() : Double.parseDouble(low.toString());
			this.high = (high instanceof Double) ? ((Double)high).doubleValue() : Double.parseDouble(high.toString());
                                                
			this.BASEBLOCKSIZE = (BASEBLOCKSIZE instanceof Double) ? ((Double)BASEBLOCKSIZE).doubleValue() : Double.parseDouble(BASEBLOCKSIZE.toString());

			// 
			this.low  = Math.floor(this.low /this.BASEBLOCKSIZE) * this.BASEBLOCKSIZE;
			this.high = Math.ceil(this.high/this.BASEBLOCKSIZE) * this.BASEBLOCKSIZE;
			if ( !(this.high > this.low) )
				throw new RuntimeException("low > high");
			this.axisSize = (int)(Math.ceil((this.high-this.low)/this.BASEBLOCKSIZE));
		}
		
		public Object low() {
			return new Double(low);
		}
		
		public Object high() {
			return new Double(high);
		}
		
		public Object BASEBLOCKSIZE() {
			return new Double(BASEBLOCKSIZE);
		}
		
		public int axisSize() {
			return this.axisSize;
		}
	
		public int getIndex(Object value, boolean checkBounds) {
			double dvalue;

			if (value instanceof Double)
				dvalue = ((Double) value).doubleValue();
			else
				dvalue = Double.parseDouble("" + value);
			if (checkBounds) {
				if (dvalue < low)
					return INDEX_TOO_SMALL;
				else if (dvalue > high)
					return INDEX_TOO_LARGE;
			}
			return (int) _floor(dvalue - this.low, this.BASEBLOCKSIZE);
//						/ this.BASEBLOCKSIZE);
//			if (exactIndex(value))
//				return (int) Math.round((dvalue - this.low)
//						/ this.BASEBLOCKSIZE);
//			else
//				return (int) Math.floor((dvalue - this.low)
//						/ this.BASEBLOCKSIZE);
		}

		public int _floor(double dvalue, double quo){
			if (_exactIndex(dvalue, quo))
				return (int) Math.round(dvalue / quo);
			else
				return (int) Math.floor(dvalue / quo);
		}
		
		public boolean exactIndex(Object value) {
			return _exactIndex(value, this.BASEBLOCKSIZE);
		}
		
		public boolean _exactIndex(Object value, double quo) {
			double dvalue;
			
			if ( value instanceof Double )
				dvalue = ((Double)value).doubleValue();
			else
				dvalue = Double.parseDouble(""+value);
			if ( dvalue < low )
				return true;
			else if ( dvalue > high )
				return true;
			// System.out.println("REMAINDER="+ Math.abs(Math.IEEEremainder(dvalue, this.BASEBLOCKSIZE)));
			return Math.abs(Math.IEEEremainder(dvalue, quo)) < 0.00000000001;
		}
		
		public Object reverseValue(int index) {
			return new Double(this.low + index * this.BASEBLOCKSIZE);
		}
		
		public String storageFormat(Object o) {
			return o.toString();
		}
		
		public String sqlType() {
			return TYPE_EXPRESSION;
		}
		
		public String sqlRangeFunction(Connection c, String func) throws SQLException {
                        String body = "";
                        
                        switch(SqlUtils.dbType(c)) {
                            case MONETDB:
                                body = "\tRETURN FLOOR((v - CAST(" + this.low + " AS double)) / CAST(" + this.BASEBLOCKSIZE + " AS double))" + ";\n";
                                break;
                            default:
                                body = "\tRETURN FLOOR((v - " + this.low + "::double precision) / " + this.BASEBLOCKSIZE + "::double precision)" + ";\n";
                                break;
                        }

			return SqlUtils.gen_Create_Or_Replace_Function(
							c, func, "v "+sqlType(), "integer",
							"", body
					);	
		}

		public AxisSplitDimension splitAxis(Object low, Object high, int cnt) {
			// 50.021|54.329403964700646|10|BBS=0.001
			LOGGER.severe("Double:1: "+low+"|"+high+"|"+cnt+"|BBS="+BASEBLOCKSIZE);
			if(cnt<=0 || low==null || high==null) throw new RuntimeException("count, low or high values are not feasible");
			double start = (Double) low;
			double end = (Double) high;
			if(end<=start) throw new RuntimeException("end value "+end+" is less than start value "+start);
			// resolve double representation errors
			int startl = getIndex(start, false);
			int endl = getIndex(end, false);
			//LOGGER.severe("Double:2: "+startl+"|"+endl+"|"+cnt);
			if(start>this.high || end<this.low)
				// query out of range of the available data
				throw new RuntimeException("query out of range of the available data: (start,end,high,low)=("+start+","+end+","+this.low+","+this.high+")");
			
			// in the case of a split along a single chunk, no alignment with the factor is possible!
//			// if(cnt==1) return new AxisSplitDimension((double) startl*BASEBLOCKSIZE, (double)endl*BASEBLOCKSIZE, 1);
			if(cnt==1) return new AxisSplitDimension(low,high,1);
			// double 	deltal = Math.ceil((endl-startl)/(cnt-1)); // WHY cnt-1???
			int deltal = (int)Math.ceil((endl-startl)/(cnt));

			// this is the case where the query is inside the available data
			
//			double _startl = (double) (_floor(startl,deltal))*deltal; // be careful with rounding here
//			double _endl = (double) (_floor(endl,deltal)+1)*deltal; // be careful with rounding here
//			
//			while(_startl<this.low/BASEBLOCKSIZE && cnt>0){
//				_startl += deltal;
//				cnt--;
//			}
//			while(_endl>this.high/BASEBLOCKSIZE && cnt>0){
//				_endl -= deltal;
//				cnt--;
//			}
			LOGGER.severe("Axis splitting result (index range): "+startl+"|"+endl+"|"+cnt);
			
			if(cnt>0)
				return new AxisSplitDimension(reverseValue(startl), reverseValue(startl+deltal), cnt);
//			if(cnt>0)
//				return new AxisSplitDimension(_startl*BASEBLOCKSIZE, (_startl+deltal)*BASEBLOCKSIZE, cnt);
			
			throw new RuntimeException("remaining count value is less than or equal to 0: "+cnt);
		}

		
	}
	
	
public class  TimestampAxisIndexer implements AxisIndexer {
		
		private long low;
		private long high;
		private int  axisSize;
		private long BASEBLOCKSIZE;
		public final static String TYPE_EXPRESSION = "timestamp with time zone";
		
		public TimestampAxisIndexer(Object low, Object high, Object BASEBLOCKSIZE) {
			this.low =  (low instanceof Timestamp) ? ((Timestamp)low).getTime() : Long.parseLong(low.toString());
			this.high = (high instanceof Timestamp) ? ((Timestamp)high).getTime() : Long.parseLong(high.toString());
			this.BASEBLOCKSIZE = (BASEBLOCKSIZE instanceof Long) ? ((Long)BASEBLOCKSIZE).longValue() : Long.parseLong(BASEBLOCKSIZE.toString());
			// 
			this.low  = (this.low /this.BASEBLOCKSIZE) * this.BASEBLOCKSIZE;
			this.high = (long)Math.ceil(this.high/this.BASEBLOCKSIZE) * this.BASEBLOCKSIZE;
			if ( !(this.high > this.low) )
				throw new RuntimeException("low > high");
			this.axisSize = (int)(Math.ceil((this.high-this.low)/this.BASEBLOCKSIZE)) + 1; // incomplete, why ???
		}
		
		public Object low() {
			return new Timestamp(low);
		}
		
		public Object high() {
			return new Timestamp(high);
		}
		
		public Object BASEBLOCKSIZE() {
			return new Long(BASEBLOCKSIZE);
		}
		
		public int axisSize() {
			return this.axisSize;
		}
	
		public int getIndex(Object value, boolean checkBounds) {
			long tsvalue;

			if (value instanceof Timestamp)
				tsvalue = ((Timestamp) value).getTime();
			else
				tsvalue = Long.parseLong("" + value);
			if (checkBounds) {
				if (tsvalue < low)
					return INDEX_TOO_SMALL;
				else if (tsvalue > high)
					return INDEX_TOO_LARGE;
			}
			return (int) ((tsvalue - this.low) / this.BASEBLOCKSIZE);
		}

		public boolean exactIndex(Object value) {
			long tsvalue;
			
			if ( value instanceof Timestamp )
				tsvalue = ((Timestamp)value).getTime();
			else
				tsvalue = Long.parseLong(""+value);
			if ( tsvalue < low )
				tsvalue = low;
			else if ( tsvalue > high )
				tsvalue = high;
			return (tsvalue % BASEBLOCKSIZE) == 0;
		}
		
		public Object reverseValue(int index) {
			return new Timestamp(this.low + index * this.BASEBLOCKSIZE);
		}
		
		public String storageFormat(Object o) {
			if ( o instanceof Timestamp )
				return ""+((Timestamp)o).getTime();
			else
				throw new RuntimeException("UNEXPECTED");
		}
		
		public String sqlType() {
			return TYPE_EXPRESSION;
		}
		
		public String sqlRangeFunction(Connection c, String fun) throws SQLException {
			double base = this.low / 1000; // epoch is in seconds, not milliseconds
			double bbs  = this.BASEBLOCKSIZE / 1000; // epoch is in seconds not milliseconds
                        
                        if (SqlUtils.dbType(c) == DbType.MONETDB) {
                            return SqlUtils.gen_Create_Or_Replace_Function(
							c, fun, "v "+sqlType(), "integer",
							"", "\tRETURN FLOOR((UNIX_TIMESTAMP(v) - " + base + ") / " + bbs + ")" + ";\n"

					);
                        } else {
                            return SqlUtils.gen_Create_Or_Replace_Function(
							c, fun, "v "+sqlType(), "integer",
							"", "\tRETURN FLOOR((EXTRACT(EPOCH FROM v) - " + base + ") / " + bbs + ")" + ";\n"

					);	
                        }
		}

		public AxisSplitDimension splitAxis(Object low, Object high, int cnt) throws RuntimeException {
			if(cnt<=0) cnt=1;
			int startl; //= this.low;
			int endl;   //= this.high;
                        
                        // ensure no nulls
                        if (low == null) low = this.low();
                        if (high == null) high = this.high();
                        
                        // convert possible long (i.e. epochs) into Timestamps
                        if (low instanceof Long)low = new Timestamp((Long)low);                        
                        if (high instanceof Long) high = new Timestamp((Long)high);
                        
                        // ensure valid Timestamp objects
                        if (!(low instanceof Timestamp)) low = this.low();                        
                        if (!(high instanceof Timestamp)) high = this.high();
                        
                        // ensure a valid low/high value
			if (((Timestamp)low).getTime() < this.low) low = this.low();
			if (((Timestamp)high).getTime() > this.high) high = this.high();
                                                
                        startl = getIndex(low, false);
			endl = getIndex(high, false);
			
			if(endl<=startl) throw new RuntimeException("end value "+endl+" is less than start value "+startl);
//			if(startl>this.high || endl<this.low)
				// query out of range of the available data
//				throw new RuntimeException("query out of range of the available data: (start,end,high,low)=("+start+","+end+","+this.low+","+this.high+")");
//			long startl = start/BASEBLOCKSIZE;
//			long endl = end/BASEBLOCKSIZE;
		
			// in the case of a split along a single chunk, no alignment with the factor is possible!
			if(cnt==1) {
				// return new AxisSplitDimension(new Timestamp(startl*BASEBLOCKSIZE), new Timestamp(endl*BASEBLOCKSIZE), 1);
				return new AxisSplitDimension(low, high, 1);
			}
			
			int deltal = (endl-startl)/(cnt-1);
			if((endl-startl)%(cnt-1) > 0) deltal++;
			//delta = delta*BASEBLOCKSIZE;
			
			// this is the case where the query is inside the available data
			long _start = (startl/deltal)*deltal;
			long _end = (endl/deltal)*deltal;
			// this is the integer version of handling CEIL function
			if(endl%deltal >0) _end++;
			_end = _end*deltal;
			
			while(_start<getIndex(this.low, false) && cnt>0){
				_start += deltal;
				cnt--;
			}
			while(_end>getIndex(this.high, false) && cnt>0){
				_end -= deltal;
				cnt--;
			}
			LOGGER.severe("axis splitter result (_start,_end,cnt)=("+_start+","+_end+","+cnt+")");
			if(cnt>0)
				// return new AxisSplitDimension(new Timestamp(_start*BASEBLOCKSIZE), new Timestamp((_start+deltal)*BASEBLOCKSIZE),cnt);
				return new AxisSplitDimension(new Timestamp(this.low+_start*BASEBLOCKSIZE), new Timestamp(this.low+(_start+deltal)*BASEBLOCKSIZE),cnt);
                        
			throw new RuntimeException("remaining count value is less than or euqal to 0: "+cnt);
		}
	}

	private AxisIndexer	indexer = null;
	private Object 		BASEBLOCKSIZE;
	private short		N;
	private String		type = null;
	
	public MetricAxis(
			String	columnExpression,
			String	type,
			Object	low,
			Object	high,
			String	BASEBLOCKSIZE,
			short	N) {
		this(columnExpression,type);
		setRangeValues(low,high,BASEBLOCKSIZE,N);
	}
	
	public MetricAxis(
			String	columnExpression,
			String	type,
			Object	BASEBLOCKSIZE,
			short	N) {
		super(columnExpression);
		this.BASEBLOCKSIZE		= BASEBLOCKSIZE;
		this.N					= N;
		this.type				= type;
	}
	
	public MetricAxis(
			String	columnExpression,
			String	type) {
		this(columnExpression,type,null,(short)-1);
	}
        
        public AxisIndexer getIndexer () {
            return this.indexer;
        }
	
	public boolean isMetric() {
		return true;
	}

	public String type() {
		return this.type;
	}

	public void setRangeValues(Object low,Object high) {
		if ( this.BASEBLOCKSIZE == null )
			throw new RuntimeException("AggregateAxis: BASEBLOCKSIZE undefined");
		if ( this.N < 0 )
			throw new RuntimeException("AggregateAxis: N undefined");
		setRangeValues(low,high,BASEBLOCKSIZE,N);
	}
	
	public void setRangeValues(Object low,Object high, Object BASEBLOCKSIZE, short	N) {
		// System.out.println("SetRangeValues(type="+type()+", low="+low+",high="+high+", BBS="+BASEBLOCKSIZE+", N="+N+")");
		this.BASEBLOCKSIZE	 = BASEBLOCKSIZE;
		this.N  = N;
		if ( type().equals("long") )
			this.indexer = new LongAxisIndexer(low,high,BASEBLOCKSIZE);
		else if ( type().equals("integer") || type().equals("int"))
			this.indexer = new IntegerAxisIndexer(low,high,BASEBLOCKSIZE);
		else if ( type().equals("double") || type().equals("double precision"))
			this.indexer = new DoubleAxisIndexer(low,high,BASEBLOCKSIZE);
		else if ( type().equals("timestamp with time zone")) {
			this.indexer = new TimestampAxisIndexer(low,high,BASEBLOCKSIZE);
		}
		else
			throw new RuntimeException("Unexpected");
	}       
	
	public boolean hasRangeValues() {
		return indexer != null;
	}
	
	public short N() {
		return this.N;
	}
	
	public Object low() {
		if ( indexer != null )
			return indexer.low(); // may be adjusted for BLOCKSIZE
		else
			throw new NullPointerException();
	}
	
	public Object high() {
		if ( indexer != null )
			return indexer.high(); // may be adjusted for BLOCKSIZE
		else
			throw new NullPointerException();
	}
	
	public Object BASEBLOCKSIZE() {
		if ( indexer != null )
			return indexer.BASEBLOCKSIZE();
		else
			throw new NullPointerException();
	}
	
	public int axisSize() {
		if ( indexer != null )
			return indexer.axisSize();
		else
			throw new NullPointerException();
	}

	public int getIndex(Object value, boolean checkBounds) {
		if ( indexer != null ) {
			int res = indexer.getIndex(value, checkBounds);
			if ( true && res < 0 ) 
				System.out.println(this+": getIndex("+value+")="+res);
			return res;
		} else
			throw new NullPointerException();
	}
	
	public Object reverseValue(int index) {
		if ( indexer != null )
			return indexer.reverseValue(index);
		else
			throw new NullPointerException();
	}
	
	public boolean exactIndex(Object value) {
		if ( indexer != null )
			return indexer.exactIndex(value);
		else
			throw new NullPointerException();
	}
	
	public short maxLevels() {
		return (short)Math.ceil(Math.log(axisSize()) / Math.log(N()));
	}
	
	public static final short log2(long base) {
		return (short) Math.ceil(Math.log(base) / Math.log((long)2));
	}
	
	public static final short pow2(int exp) {
		return (short) Math.round(Math.pow(2,exp));
	}
	
	public short bits() {
		/* The real axis size has to make room for 2 extra values for every index of a dimension:
		 * 0 - value is too small
		 * 2^n - 1 - value is too big
		 */
		int real_axis_size = axisSize() + 2;
		return log2(real_axis_size);
	}
	
	protected int tooLow() {
		return 0;
	}
	
	protected int tooHigh() {
		return pow2(bits()) - 1;
	}

	public int dimensionKeyValue(int d_i) {
		if ( d_i < 0 ) {
			if ( d_i == INDEX_TOO_SMALL )
				return tooLow();
			else if ( d_i == INDEX_TOO_LARGE )
				return tooHigh();
			else 
				throw new RuntimeException("bad dimension key value: "+d_i);
		} else
			return d_i + 1;
		
	}
	
	public String storageFormat(Object o) {
		if ( indexer != null )
			return indexer.storageFormat(o);
		else
			throw new NullPointerException();
	}
	
	public String sqlType() {
		return indexer.sqlType();	
	}
	
	public String sqlRangeFunction(Connection c, String fun) throws SQLException {
		return indexer.sqlRangeFunction(c, fun);	
	}
	
	/**
	 * function does not take care of baseblocksize for the boundatries of the split
	 * and the delta/chunk
	 * @param n
	 * @return
	 */
	public Object[][] split(int n) {
		Object[][] res = new Object[n][2];
		
		int sz = axisSize();
		int chunk = sz / n; 
		for(int i=0; i<n; i++) {
			if ( i == 0 )
				res[i][0] = low();
			else
				res[i][0] = reverseValue(i*chunk);
			if ( i == (n-1) )
				res[i][1] = high();
			else
				res[i][1] = reverseValue((i+1)*chunk);
		}
		return res;
	}
	
	public String toString() {
		return "AggregateAxis(colExpr="+columnExpression()+", type="+sqlType()+", low="+low()+",high="+high()+", BBS="+indexer.BASEBLOCKSIZE()+", N="+N()+",axisSize="+axisSize()+", bits="+bits()+", maxLevels="+maxLevels()+")";
	}

	public AxisSplitDimension splitAxis(Object low, Object high, int cnt) {
		if ( indexer != null )
			return indexer.splitAxis(low, high, cnt); 
		else
			throw new NullPointerException();
	}

}

