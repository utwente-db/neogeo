import org.jfree.data.xy.YIntervalSeriesCollection;


public class JdbcYIntervalSeriesCollection extends YIntervalSeriesCollection {
	private double start;
	private double extent;
	private double ds_start = 0;
	private double ds_extent = 0;
	private int ds_quantile;

	protected int MAX_RESOLUTION = 100;

	/**
	 * specify the start and the extent of the data 
	 * @param start
	 * @param extent
	 */
	public void update(long start, long extent){
		long factor = (long) Math.ceil(extent/MAX_RESOLUTION);
		this.start = start;
		this.extent = extent;
		int quantile = 0;
		long ds_factor = (long) Math.ceil(ds_extent/MAX_RESOLUTION);
//		if (start < ds_start || start > ds_start+ds_extent || 
//				start+extent > ds_start+ds_extent ||
//				factor < ds_factor/2 || factor > ds_factor*2 ||
//				quantile != ds_quantile){
			System.out.print("update with start, extent, factor, quantile, querytime: "+
						start+","+extent+","+factor+","+quantile);
			for(int i=0; i<getSeriesCount(); i++){
				JdbcYIntervalSeries series = (JdbcYIntervalSeries) getSeries(i);
				series.update(start, extent, factor);
			}
			this.ds_start = start-extent;
			this.ds_extent = start+2*extent;
			this.ds_quantile = quantile;
//		}
		
	}
	
	public long getFactor(){
		return (long) Math.ceil(extent/MAX_RESOLUTION);
	}
	
	public double getStart(){
		return start;
	}
	
	public double getExtent(){
		return extent;
	}
	
	public int getQuantile(){
		return ds_quantile;
	}
}
