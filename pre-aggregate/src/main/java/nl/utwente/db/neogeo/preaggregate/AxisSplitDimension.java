package nl.utwente.db.neogeo.preaggregate;

public class AxisSplitDimension {
	/**
	 * number of elements in the grid along this dimension 
	 */
	private int cnt;

	/**
	 * start of the first grid cell;
	 * is a multiple of the BASEBLOCKSIZE
	 */
	private Object start;

	/**
	 * end point of the grid;
	 * is a multiple of the BASEBLOCKSIZE
	 */
	private Object end;
	
	public AxisSplitDimension(Object start, Object end, int cnt){
		this.start = start;
		this.end = end;
		this.cnt = cnt;
	}
	
	public int getCount(){
		return cnt;
	}
	
	public Object getStart(){
		return start;
	}
	
	public Object getEnd(){
		return end;
	}

	@Override
	public String toString(){
		return "(start:"+start.toString()+"|end:"+end.toString()+"|cnt:"+cnt+")";
	}
}
