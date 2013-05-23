package nl.utwente.db.neogeo.preaggregate.mysql;

public class OrderQuery {

	// start of the query
	private long s;
	// end of the query
	private long e;
	// level of the pre-aggregate
	private long l0;
	// index of the pre-aggregate
	private long i0;
	// offset of the pre-aggregate
	private int offset;
	// start of the bucket in the preaggregate
	private long st;
	//end of the bucket in the pre-aggregate
	private long et;
	
	private long soff;
	private long eoff;
	
	public OrderQuery(long s, long e, long l0, long i0, int offset, long st, long et){
		this.s = s;
		this.e = e;
		this.l0 = l0;
		this.i0 = i0;
		this.offset = offset;
		this.st = st;
		this.et = et;
	}

	public long getS() {
		return s;
	}

	public long getE() {
		return e;
	}

	public long getL0() {
		return l0;
	}

	public long getI0() {
		return i0;
	}

	public int getOffset() {
		return offset;
	}

	public long getSt() {
		return st;
	}

	public long getEt() {
		return et;
	}
	
	public void setOverhead(long soff, long eoff){
		this.soff = soff;
		this.eoff = eoff;
	}

	public long getSoff() {
		return soff;
	}

	public long getEoff() {
		return eoff;
	}
	
}
