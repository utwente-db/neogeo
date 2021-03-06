package nl.utwente.db.neogeo.preaggregate;

public class PermutationGenerator {
	
	// incomplete, should implement a step and a including ub
	
	// incomplete, implement this in C with a pmut prefix
	
	private static final boolean verbose = false;
	
	private int dimensions;
	private int	range_low[]		= null;
	private int	range_high[]	= null;
	
	public PermutationGenerator(int dimensions) {
		if ( verbose )
			System.out.println("#PG.constructor("+dimensions+")");
		this.dimensions = dimensions;
		this.range_low	= new int[dimensions];
		this.range_high	= new int[dimensions];
		reset();
	}
	
	public void reset() {
		for(int i=0; i<dimensions; i++)
			setRange(i,-1,-1);
	}

	public void setRange(int dim, int low, int high) {
		if ( low > high )
			throw new RuntimeException("RANGE ERROR");
		if ( verbose )
			System.out.println("#PG.setRange("+dim+","+low+","+high+")");
		range_low[dim]	= low;
		range_high[dim]	= high;
	}
	
	int permutation[] = null;
	int curDim = -1;
	boolean end_of_p = true;

	public void start() {
		if (cardinality() > 0) {
			permutation = new int[dimensions];
			for (int i = 0; i < dimensions; i++)
				permutation[i] = range_low[i];
			curDim = dimensions - 1;
			permutation[curDim]--;
			end_of_p = false;
		} else {
			end_of_p = true;
		}
	}
	
	public boolean next() {
		if ( end_of_p )
			return false;
		if ( ++permutation[curDim] >= range_high[curDim] ) {
			while ( curDim >0 ) {
				permutation[curDim] = range_low[curDim];
				--curDim;
				// System.out.println("curdim="+curDim+" v="+permutation[curDim]);
				if ( ++permutation[curDim] < range_high[curDim] ) {
					curDim = dimensions - 1;
					return true;
				}
			} 
			permutation = null;
			curDim = -1;
			end_of_p = true;
			return false;
		}
		return true;
	}
	
	public int permutation(int i) {
		return permutation[i];
	}
	
	public int cardinality() {
		int res = 1;
		for(int i=0; i < dimensions; i++)
			res *= (range_high[i] - range_low[i] );
		return res;
	}
	
//	public static void main(String[] argv) {
//		PermutationGenerator p = new PermutationGenerator(3);
//		
//		p.setRange(0,0,5);
//		p.setRange(1,11,14);
//		p.setRange(2,101,104);
//		p.start();
//		while ( p.next() ) {
//			System.out.println("> "+p.permutation(0)+" "+p.permutation(1)+" "+p.permutation(2));
//		}
//		System.out.println("The number of permutations is "+p.cardinality());
//	}
//	
	
}
