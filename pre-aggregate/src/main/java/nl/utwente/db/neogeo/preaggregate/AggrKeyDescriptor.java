package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
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
	public short	totalBits = -1;
	
	public	AggrKeyDescriptor(char kind, AggregateAxis axis[]) {
		_init(kind, axis);
	}
	
	protected void _init(char kind, AggregateAxis axis[]) {
		this.kind		= kind;
		switch ( kind ) {
		 case KD_NULL:
			_init(KD_CROSSPRODUCT_LONG, axis);
		    break;
		 case KD_CROSSPRODUCT_LONG:
			 this.dimensions = (short)axis.length;
			 this.computeBitLayout(kind, axis);
			 break;
		 case KD_BYTE_STRING:
			 throw new RuntimeException("INCOMPLETE");
		 default:
			 throw new RuntimeException("bad kind");
		}
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
	
	private void computeBitLayout(char kind, AggregateAxis axis[]) {
		dimBits = new short[axis.length];
		
		totalBits = 0;
		for(int i=0; i<axis.length; i++) {
			dimBits[i] = axis[i].bits();
			totalBits += dimBits[i];
			if ( axis[i].maxLevels() > maxLevel )
				maxLevel = axis[i].maxLevels();
		}
		levelBits = AggregateAxis.log2(maxLevel);
		if ( kind == KD_CROSSPRODUCT_LONG )
			totalBits += (axis.length * levelBits);
		else
			totalBits += levelBits;
		if ( totalBits > 63 )
			throw new RuntimeException("no of bits in key > 63");
	}
	
	public String crossproductLongKeyFunction(Connection c, String fun) throws SQLException {	
		String sres = "0";
		StringBuilder pars = new StringBuilder();
		
		for(short i=0; i<dimensions; i++) {
			if (i>0)
				pars.append(',');
			pars.append("l"+i+" int,i"+i+" int");
			sres = "("+sres+")" + "*" +  (int)Math.pow(2,levelBits) + "+" + "l"+i;
			sres = "("+sres+")" + "*" +  (int)Math.pow(2,dimBits[i]) + "+" + "i"+i;

		}
		return SqlUtils.gen_Create_Or_Replace_Function(
							c, fun, pars.toString(), "bigint",
							// "\tDECLARE start bigint := 0;\n" +
							"\tRETURN "+sres+";\n"
				);	
	}
	
	public String crossproductLongKeyFunctionOLD(String fun) {	
		String sres = "start";
		StringBuilder pars = new StringBuilder();
		
		for(short i=0; i<dimensions; i++) {
			if (i>0)
				pars.append(',');
			pars.append("l"+i+" int,i"+i+" int");
			sres = "("+sres+")" + "*" +  (int)Math.pow(2,levelBits) + "+" + "l"+i;
			sres = "("+sres+")" + "*" +  (int)Math.pow(2,dimBits[i]) + "+" + "i"+i;

		}
		StringBuilder res = new StringBuilder();
		
		res.append(fun);
		res.append("("+pars+") RETURNS int8 AS $$\n");
		res.append("DECLARE start int8 := 0;\n");
		res.append("BEGIN\n");
		res.append("\tRETURN "+sres+";\n");
		res.append("END\n");
		res.append("$$ LANGUAGE plpgsql");
		return res.toString();	
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
}
