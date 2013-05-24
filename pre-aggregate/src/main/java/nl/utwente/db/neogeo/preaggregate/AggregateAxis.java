package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;

public class AggregateAxis {
	
	interface AxisIndexer {
		public Object 	low();
		public Object 	high();
		public Object 	BASEBLOCKSIZE();
		public int		axisSize();
		public int	   	getIndex(Object value);
		public boolean	exactIndex(Object value);
		public Object 	reverseValue(int index);
		public String   storageFormat(Object o);
		public String	sqlType();
		public String   sqlRangeFunction(Connection c, String name) throws SQLException ;
	}
	
class  IntegerAxisIndexer implements AxisIndexer {
		
		private int low;
		private int high;
		private int axisSize;
		private int BASEBLOCKSIZE;
		
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
	
		public int getIndex(Object value) {
			int lvalue;
			
			if ( value instanceof Integer )
				lvalue = ((Integer)value).intValue();
			else
				lvalue = Integer.parseInt(""+value);
			if ( lvalue < low )
				lvalue = low;
			else if ( lvalue > high )
				lvalue = high;
			return (int)((lvalue-this.low)/this.BASEBLOCKSIZE);
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
			return "integer";
		}
		
		public String sqlRangeFunction(Connection c, String fun) {
			StringBuilder res = new StringBuilder();
			
			res.append(fun);
			res.append("(v "+sqlType()+") RETURNS integer AS $$\n");
			res.append("BEGIN\n");
			res.append("\tRETURN CAST( ( (v - " + this.low + ") / " + this.BASEBLOCKSIZE + ") AS integer);\n");
			res.append("END\n");
			res.append("$$ LANGUAGE plpgsql");
			return res.toString();	
		}
		
	}
	
class  LongAxisIndexer implements AxisIndexer {
		
		private long low;
		private long high;
		private int    axisSize;
		private long BASEBLOCKSIZE;
		
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
	
		public int getIndex(Object value) {
			long lvalue;
			
			if ( value instanceof Long )
				lvalue = ((Long)value).longValue();
			else
				lvalue = Long.parseLong(""+value);
			if ( lvalue < low )
				lvalue = low;
			else if ( lvalue > high )
				lvalue = high;
			return (int)((lvalue-this.low)/this.BASEBLOCKSIZE);
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
			return "bigint";
		}
		
		public String sqlRangeFunction(Connection c, String fun) throws SQLException {
			return SqlUtils.gen_Create_Or_Replace_Function(
							c, fun, "v "+sqlType(), "integer",
							"\tRETURN " + SqlUtils.gen_DIV(c,"v - " + this.low, ""+this.BASEBLOCKSIZE) + ";"
					);	
		}
		
	}
	
	class  DoubleAxisIndexer implements AxisIndexer {
		
		private double low;
		private double high;
		private int    axisSize;
		private double BASEBLOCKSIZE;
		
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
	
		public int getIndex(Object value) {
			double dvalue;
			
			if ( value instanceof Double )
				dvalue = ((Double)value).doubleValue();
			else
				dvalue = Double.parseDouble(""+value);
			if ( dvalue < low )
				dvalue = low;
			else if ( dvalue > high )
				dvalue = high;
			return (int)((dvalue-this.low)/this.BASEBLOCKSIZE);
		}
		
		public boolean exactIndex(Object value) {
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
			return Math.abs(Math.IEEEremainder(dvalue, this.BASEBLOCKSIZE)) < 0.0000000000001;
		}
		
		public Object reverseValue(int index) {
			return new Double(this.low + index * this.BASEBLOCKSIZE);
		}
		
		public String storageFormat(Object o) {
			return o.toString();
		}
		
		public String sqlType() {
			return "double precision";
		}
		
		public String sqlRangeFunction(Connection c, String fun) {
			StringBuilder res = new StringBuilder();
			
			res.append("CREATE OR REPLACE FUNCTION "+ fun +"(v "+sqlType()+") RETURNS integer AS $$\n");
			res.append("BEGIN\n");
			res.append("\tRETURN CAST( ( (v - " + this.low + ") / " + this.BASEBLOCKSIZE + ") AS integer);\n");
			res.append("END\n");
			res.append("$$ LANGUAGE plpgsql");
			return res.toString();	
		}
		
	}
	
	
class  TimestampAxisIndexer implements AxisIndexer {
		
		private long low;
		private long high;
		private int  axisSize;
		private long BASEBLOCKSIZE;
		
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
	
		public int getIndex(Object value) {
			long tsvalue;
			
			if ( value instanceof Timestamp )
				tsvalue = ((Timestamp)value).getTime();
			else
				tsvalue = Long.parseLong(""+value);
			if ( tsvalue < low )
				tsvalue = low;
			else if ( tsvalue > high )
				tsvalue = high;
			return (int)((tsvalue-this.low)/this.BASEBLOCKSIZE);
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
			return (tsvalue * BASEBLOCKSIZE) == 0;
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
			return "timestamp with time zone";
		}
		
		public String sqlRangeFunction(Connection c, String fun) {
			StringBuilder res = new StringBuilder();
			
			res.append(fun);
			res.append("(v "+sqlType()+") RETURNS integer AS $$\n");
			res.append("BEGIN\n");
			double base = this.low / 1000; // epoch is in seconds, not milliseconds
			double bbs  = this.BASEBLOCKSIZE / 1000; // epoch is in seconds not milliseconds
			// res.append("\tRETURN CAST((EXTRACT(EPOCH FROM ( (v - to_timestamp(" + base + ")))) / " + bbs + ") AS integer);\n");
			res.append("\tRETURN CAST(((EXTRACT(EPOCH FROM v) - " + base + ") / " + bbs + ") AS integer);\n");
			res.append("END\n");
			res.append("$$ LANGUAGE plpgsql");
			return res.toString();	
		}
		
	}

	private AxisIndexer	indexer;
	private String		columnExpression; // eg. attributename 'value' or Attribute expression like ST_X(coordinate)
	private String		type;
	private Object 		BASEBLOCKSIZE;
	private short		N;
	
	public AggregateAxis(
			String	columnExpression,
			String	type,
			String	low,
			String	high,
			String	BASEBLOCKSIZE,
			short	N) {
		this(columnExpression,type);
		setRangeValues(low,high,BASEBLOCKSIZE,N);
	}
	
	public AggregateAxis(
			String	columnExpression,
			String	type,
			Object	BASEBLOCKSIZE,
			short	N) {
		this.columnExpression	= columnExpression;
		this.type				= type;
		this.BASEBLOCKSIZE		= BASEBLOCKSIZE;
		this.N					= N;
	}
	
	public AggregateAxis(
			String	columnExpression,
			String	type) {
		this(columnExpression,type,null,(short)-1);
	}
	
	public void setRangeValues(Object low,Object high) {
		if ( this.BASEBLOCKSIZE == null )
			throw new RuntimeException("AggregateAxis: BASEBLOCKSIZE undefined");
		if ( this.N < 0 )
			throw new RuntimeException("AggregateAxis: N undefined");
		setRangeValues(low,high,BASEBLOCKSIZE,N);
	}
	
	public void setRangeValues(Object low,Object high, Object BASEBLOCKSIZE, short	N) {
		// System.out.println("SetRangeValues(type="+type+", low="+low+",high="+high+", BBS="+BASEBLOCKSIZE+", N="+N+")");
		this.BASEBLOCKSIZE	 = BASEBLOCKSIZE;
		this.N  = N;
		if ( type.equals("long") )
			this.indexer = new LongAxisIndexer(low,high,BASEBLOCKSIZE);
		else if ( type.equals("integer") || type.equals("int"))
			this.indexer = new IntegerAxisIndexer(low,high,BASEBLOCKSIZE);
		else if ( type.equals("double") || type.equals("double precision"))
			this.indexer = new DoubleAxisIndexer(low,high,BASEBLOCKSIZE);
		else if ( type.equals("timestamp with time zone")) {
			this.indexer = new TimestampAxisIndexer(low,high,BASEBLOCKSIZE);
		}
		else
			throw new RuntimeException("Unexpected");
	}
	
	public boolean hasRangeValues() {
		return indexer != null;
	}
	
	public String columnExpression() {
		return this.columnExpression;
	}
	
	public String type() {
		return this.type;
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

	public int getIndex(Object value) {
		if ( indexer != null )
			return indexer.getIndex(value);
		else
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
		//return (short)Math.ceil(Math.pow(axisSize(), 1.0/(double)N()));
		return (short)Math.ceil(Math.log(axisSize()) / Math.log(N()));
	}
	
	public static final short log2(long base) {
		return (short) Math.ceil(Math.log(base) / Math.log((long)2));
	}
	
	public static final short pow2(int base) {
		return (short) Math.round(Math.pow(base, 2));
	}
	
	public short bits() {
		return log2(axisSize());
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
	
	public String toString() {
		return "AggregateAxis(colExpr="+columnExpression+", type="+sqlType()+", low="+low()+",high="+high()+", BBS="+indexer.BASEBLOCKSIZE()+", N="+N()+",axisSize="+axisSize()+", bits="+bits()+", maxLevels="+maxLevels()+")";
	}
	
}
