package nl.utwente.db.neogeo.preaggregate;

import java.util.Arrays;

public final class AggrKey {
	
	private AggrKeyDescriptor kd;
	private int[]	data;
	
	public AggrKey(AggrKeyDescriptor kd) {
		this(kd,new int[kd.dimensions()*2]);	
	}
	
	public AggrKey(AggrKeyDescriptor kd, int data[]) {
		if ( (kd == null) || (data == null) ) {
			throw new NullPointerException();
		}
		this.kd	   = kd;
		this.data  = data;
	}
	
	public final void reset() {
		for(int i=0; i<data.length; i++) {
			data[i] = 0;
		}
	}
	
	public AggrKeyDescriptor kd() {
		return this.kd;
	}
	
	public AggrKey copy() {
		return new AggrKey(kd, data.clone());
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof AggrKey)) {
			return false;
		}
		return Arrays.equals(data, ((AggrKey) other).data);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(data);
	}
	
	/*
	 * 
	 * 
	 */
	
	public void setIndex(short dim, int i) {
		this.data[dim] = i;
	}
	
	public void setLevel(short dim, short l) {
		this.data[kd.dimensions()+dim] = (int)l;
	}
	
	public int getIndex(short dim) { // should maybe be called range
		return data[dim];	
	}
	
	public short getLevel(short dim) {
		return (short)this.data[kd.dimensions()+dim];
	}
	
	public boolean isSubindexed() {
		return kd.isSubindexed();
	}
	
	public long crossproductLongKey() {	
		long res = 0;
		// long nres = 0;
		
		for(short i=0; i<kd.dimensions(); i++) {	
			res = ( (res << kd.levelBits) + (long)getLevel(i) );
			res = ( (res << kd.dimBits[i]) + kd.axis[i].dimensionKeyValue(getIndex(i)) );
		}
		// System.out.println("# "+toString()+"="+res+"[nres="+nres+"]");
		return res;
	}
	
	public Object toKey() {
		switch (kd.kind()) {	
		 case AggrKeyDescriptor.KD_CROSSPRODUCT_LONG:
			 return new Long(crossproductLongKey());
		 case AggrKeyDescriptor.KD_BYTE_STRING:
			 throw new RuntimeException("BYTE STRING LONG NOT IMPL");
		 default:
			 throw new RuntimeException("UNEXPECTED");
		}
	}
	
	public String toString() {
		short i;
		StringBuilder sb;

		if (kd == null)
			throw new NullPointerException();
		sb = new StringBuilder();
		sb.append("AggrKey<kd=");
		sb.append(kd.toString());
		sb.append(",i=[");
		for (i = 0; i < kd.dimensions(); i++) {
			if (i > 0)
				sb.append(",");
			sb.append(getIndex(i));
		}
		sb.append("]");
		if (kd.isSubindexed()) {
			sb.append(",s=[");
			for (i = 0; i < kd.dimensions(); i++) {
				if (i > 0)
					sb.append(",");
				sb.append(getLevel(i));
			}
			sb.append("]");
		}
		sb.append(")>");
		return sb.toString();
	}
	
}
