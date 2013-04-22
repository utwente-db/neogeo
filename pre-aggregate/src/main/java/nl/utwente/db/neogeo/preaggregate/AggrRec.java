package nl.utwente.db.neogeo.preaggregate;

public interface AggrRec {

	public abstract AggrRec copy();
	
	public abstract void add(AggrRec toAdd);
	
	public abstract long getCount();
	public abstract Object getSum();
	public abstract Object getMin();
	public abstract Object getMax();

}
