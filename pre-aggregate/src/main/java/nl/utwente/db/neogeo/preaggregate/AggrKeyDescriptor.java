package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AggrKeyDescriptor {
	
	public static final boolean defaultSubindexed	= true;
	
	public static final char KD_NULL				= '\0';
	public static final char KD_CROSSPRODUCT_LONG	= 'X';
	public static final char KD_BYTE_STRING			= 'B';

	private char	kind;
	private short	dimensions;
	private boolean	subindexOn = true;
	
	public short	maxLevel  = -1;
	public short	levelBits = -1;
	public short	dimBits[] = null;
        
        public short    levelBytes = -1;
	public short	totalBits = -1;
        public short    totalBytes = -1;
        public short    dimBytes[] = null;
	
	public AggregateAxis axis[];
	
	public	AggrKeyDescriptor(char kind, AggregateAxis axis[]) throws TooManyBitsException {
		_init(kind, axis);
	}
	
	protected void _init(char kind, AggregateAxis axis[]) throws TooManyBitsException {
		this.kind		= kind;
		this.axis		= axis;
                this.dimensions = (short)axis.length;
		switch ( kind ) {
		 case KD_NULL:
			_init(KD_CROSSPRODUCT_LONG, axis);
		    break;
		 case KD_CROSSPRODUCT_LONG:
			 this.computeBitLayout(kind, axis);
			 break;
		 case KD_BYTE_STRING:
                        this.computeByteLayout(kind, axis);
			 break;
		 default:
			 throw new RuntimeException("bad kind");
		}
	}
        
        public short getTotalBits () {
            return this.totalBits;
        }
        
        public short getTotalBytes () {
            return this.totalBytes;
        }
        
        public short getLevelBytes () {
            return this.levelBytes;
        }
	
	public char kind() {
		return this.kind;
	}
	
	public int dimensions() {
		return dimensions;
	}
	
	public boolean isSubindexed() {
		return subindexOn && ((kind == KD_CROSSPRODUCT_LONG));
	}
	
	public void switchSubindexOff() {
		subindexOn = false;
	}
	
	public void subindexAssert() {
	}
        
        private void computeByteLayout (char kind, AggregateAxis axis[]) {
            dimBytes = new short[axis.length];
            
            totalBytes = 0;
            for(int i=0; i < axis.length; i++) {
                short axisBits = axis[i].bits();   
                
                System.out.println("Axis " + i + " bits: " + axisBits);
                
                // determine what type this dimension needs
                if (axisBits <= 8) {
                    dimBytes[i] += 1; // can be stored in a single byte
                } else if (axisBits <= 16) {
                    dimBytes[i] += 2; // short, so 2 bytes
                } else if (axisBits <= 24) {
                    dimBytes[i] = 3; // in-between
                } else if (axisBits <= 32) {
                    dimBytes[i] += 4; // int, so 4 bytes
                } else if (axisBits <= 64) {
                    dimBytes[i] += 8; // long, so 8 bytes
                } else {
                    // what?! needs more than long? not supported
                    throw new RuntimeException("Axis " + axis[i].columnExpression() + " needs more bits than supported by a long");
                }
                
                totalBytes += dimBytes[i];
                
                // calculate max level
                if (axis[i].maxLevels() > maxLevel) maxLevel = axis[i].maxLevels();
            }
            
            if (maxLevel <= Byte.MAX_VALUE) {
                levelBytes = 1;
            } else if (maxLevel <= Short.MAX_VALUE) {
                levelBytes = 2;
            } else if (maxLevel <= Integer.MAX_VALUE) {
                levelBytes = 4;
            } else if (maxLevel <= Long.MAX_VALUE) {
                levelBytes = 8;
            } else {
                // what?! level is bigger than long, should be impossible
                throw new RuntimeException("MaxLevel is bigger than Long.MAX_VALUE");
            }
            
            totalBytes += (levelBytes * axis.length);
        }
	
	private void computeBitLayout(char kind, AggregateAxis axis[]) throws TooManyBitsException {
		dimBits = new short[axis.length];
		
		totalBits = 0;
		for(int i=0; i<axis.length; i++) {
			dimBits[i] = axis[i].bits();
			totalBits += dimBits[i];
			if ( axis[i].maxLevels() > maxLevel )
				maxLevel = axis[i].maxLevels();
		}
		levelBits = MetricAxis.log2(maxLevel);
		if ( kind == KD_CROSSPRODUCT_LONG )
			totalBits += (axis.length * levelBits);
		else
			totalBits += levelBits;
		if ( totalBits > 63 )
			throw new TooManyBitsException("no of bits in key > 63");
	}
        
	public String crossproductLongKeyFunction(Connection c, String fun) throws SQLException {	
		String sres = "startVar";
		StringBuilder pars = new StringBuilder();
		
		for(short i=0; i<dimensions; i++) {
			if (i>0)
				pars.append(',');
			pars.append("l"+i+" integer,i"+i+" integer");
			sres = "("+sres+")" + "*" +  (int)Math.pow(2,levelBits) + "+" + "l"+i;
			sres = "("+sres+")" + "*" +  (int)Math.pow(2,dimBits[i]) + "+" + "(i"+i+"+1)";
		}
		return SqlUtils.gen_Create_Or_Replace_Function(
							c, fun, pars.toString(), "bigint",
							"DECLARE startVar bigint;\n",
							"\t" + SqlUtils.sql_assign(c,"startVar","0")+";\n" +
							"\tRETURN "+sres+";\n"
				);	
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("KD<"+kind+","+dimensions);
		if ( dimBits != null ) {
			sb.append(",levelBits="+levelBits);
			sb.append(",bits[");
			for(short i=0; i<dimBits.length; i++) {
				if ( i>0 )
					sb.append("<<");
				sb.append(dimBits[i]);
			}
			sb.append("],totalBits="+totalBits);
		}
		sb.append(">");
		return sb.toString();
	}
        
        public class TooManyBitsException extends Exception {
            public TooManyBitsException (String msg) {
                super(msg);
            }
        }
}
