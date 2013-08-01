package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AggregateAxis {
	
	public static final int INDEX_TOO_SMALL = -2;
	public static final int INDEX_TOO_LARGE = -1;
	
	private String columnExpression;
	private String type;
	
	protected AggregateAxis(String columnExpression, String type) {
		this.columnExpression = columnExpression;
		this.type = type;
	}
	
	public String columnExpression() {
		return this.columnExpression;
	}
	
	public String type() {
		return this.type;
	}
	
	public abstract boolean isMetric();
	
	public abstract short N();
	
	// public abstract Object low();
	
	// public abstract Object high();
	
	public abstract int axisSize();
	
	public abstract int getIndex(Object value, boolean checkBounds);
	
	public abstract Object reverseValue(int index);
	
	// public abstract boolean exactIndex(Object value);
	
	public abstract short maxLevels();
	
	public abstract short bits();
	
	// protected abstract int tooLow();
	
	// protected abstract int tooHigh();

	public abstract int dimensionKeyValue(int d_i);

	
	public abstract String storageFormat(Object o);
	
	public abstract String sqlType();
	
	public abstract String sqlRangeFunction(Connection c, String fun) throws SQLException;
	
	/**
	 * function does not take care of baseblocksize for the boundatries of the split
	 * and the delta/chunk
	 * @param n
	 * @return
	 */
	
	public abstract String toString();

	public abstract AxisSplitDimension splitAxis(Object low, Object high, int cnt);

}
