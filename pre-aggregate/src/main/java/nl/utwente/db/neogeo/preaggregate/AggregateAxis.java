package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AggregateAxis {
	
	public static final int INDEX_TOO_SMALL = -2;
	public static final int INDEX_TOO_LARGE = -1;
	
	private String columnExpression;
	
	protected AggregateAxis(String columnExpression /*, String type*/) {
		this.columnExpression = columnExpression;
	}
	
	public String columnExpression() {
		return this.columnExpression;
	}
	
	public abstract boolean isMetric();
	
	public abstract String type();
	
	public abstract short N();
	
	public abstract int axisSize();
        	
	public abstract int getIndex(Object value, boolean checkBounds);
	
	public abstract Object reverseValue(int index);
	
	public abstract short maxLevels();
	
	public abstract short bits();

	public abstract int dimensionKeyValue(int d_i);
	
	public abstract String storageFormat(Object o);
	
	public abstract String sqlType();
	
	public abstract String sqlRangeFunction(Connection c, String fun) throws SQLException;
	
	/**
	 * function does not take care of baseblocksize for the boundaries of the split
	 * and the delta/chunk
	 * @param n
	 * @return
	 */
	
	public abstract String toString();

	public abstract AxisSplitDimension splitAxis(Object low, Object high, int cnt);
}
