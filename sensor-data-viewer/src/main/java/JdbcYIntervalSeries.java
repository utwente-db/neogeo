import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.jfree.data.Range;
import org.jfree.data.time.Second;
import org.jfree.data.xy.YIntervalSeries;


public class JdbcYIntervalSeries extends YIntervalSeries {

	private Connection con;
	private String url;
	private String driverName;
	private String user;
	private String password;
	private String xAttribute;
	private String yAttribute;
	private String tableName;
	private String constraint;
//	private double ds_start = 0;
//	private double ds_extent = 0;
	private boolean flag0 = true;
	private boolean flag1 = true;
	private boolean flag2 = true;
	
	protected int MAX_RESOLUTION = 100;

	public JdbcYIntervalSeries(Comparable key) {
		super(key);
	}

	/**
	 * Creates a new dataset (initially empty) using the specified database connection.
	 * @param con
	 */
	public JdbcYIntervalSeries(Comparable key, Connection con){
		super(key);
		this.con=con;
	}

	/**
	 * Creates a new dataset using the specified database connection, 
	 * and populates it using data obtained with the supplied query. 
	 * @param con
	 * @param xAttribute
	 * @param yAttribute
	 * @param tableName
	 * @param constraint
	 */
	public JdbcYIntervalSeries(Comparable key, Connection con, 
			String xAttribute, String yAttribute, String tableName, String constraint){
		super(key);
		this.con = con;
		this.xAttribute=xAttribute;
		this.yAttribute = yAttribute;
		this.tableName = tableName;
		this.constraint = constraint;
	}


	/**
	 * Creates a new dataset (initially empty) and establishes a new database connection. 
	 * @param key
	 * @param url
	 * @param driverName
	 * @param user
	 * @param password
	 * @param xAttribute
	 * @param yAttribute
	 * @param tableName
	 * @param constraint
	 */
	public JdbcYIntervalSeries(Comparable key, String url, String driverName, String user, String password,
			String xAttribute, String yAttribute, String tableName, String constraint){
		super(key);
		this.url = url;
		this.driverName = driverName;
		this.user = user;
		this.password = password;
		getConnection();
		this.xAttribute=xAttribute;
		this.yAttribute = yAttribute;
		this.tableName = tableName;
		this.constraint = constraint;
	}

	/**
	 * return an existing connection. If the connection does not exists a new connection 
	 * is established.
	 * @return connection object
	 */
	protected Connection getConnection(){
		if(con==null)
			try {
				//Register the JDBC driver for MySQL.
				Class.forName(driverName);
				con = DriverManager.getConnection(url,user, password);
			} catch(Exception e){
				e.printStackTrace();
			}
			return con;
	}

	/**
	 * the range of the domain i.e. the x axis; 
	 * this is the overall range and not the range of the displayed data 
	 * @return range of the x axis
	 */
	public Range getDomainRange(){
		long maximumItemCount = 0;
		long minimumItemCount = 0;
		Connection con = getConnection();
		if(con==null) return null;
		Statement st;
		try {
			st = con.createStatement();
			String query = "select min(`"+xAttribute+"`) as MIN ,max(`"+xAttribute+"`) as MAX from "+tableName;
			if(constraint!=null && !constraint.isEmpty()) query+= " where "+constraint;
			ResultSet rs = st.executeQuery(query);
			rs.next();
			minimumItemCount = rs.getLong(1);
			maximumItemCount = rs.getLong(2);
			//			update(minimumItemCount,maximumItemCount);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new Range(minimumItemCount, maximumItemCount);
	}

	/**
	 * the range of the y axis; 
	 * this is the overall range and not the range of the displayed data 
	 * @return range of the y axis
	 */
	public Range getYRange(){
		double maximumItemCount = 0;
		double minimumItemCount = 0;
		Connection con = getConnection();
		if(con==null) return null;
		Statement st;
		try {
			st = con.createStatement();
			String query = "select min(`"+yAttribute+"`) as MIN ,max(`"+yAttribute+"`) from "+tableName;
			if(constraint!=null && !constraint.isEmpty()) query+= " where "+constraint;
			ResultSet rs = st.executeQuery(query);
			rs.next();
			maximumItemCount = rs.getDouble("MAX");
			minimumItemCount = rs.getDouble("MIN");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new Range(minimumItemCount, maximumItemCount);
	}

	/**
	 * specify the start and the extent of the data 
	 * @param start
	 * @param extent
	 */
	public void update(long start, long extent, long factor){
		//long factor = (long) Math.ceil(extent/MAX_RESOLUTION);
		System.out.print("update with start, extent, factor, querytime: "+
				start+","+extent+","+factor);
		this.data.clear();			
		// load the data
		Connection con = getConnection();
		Object obj;
		if(con==null) return; 
		Statement st;
		try {
			String query0,query1,query2;
			//if(flag0)
			//query0 = "select "+xAttribute+", ID, "+yAttribute+","+yAttribute+","+yAttribute+" from "+tableName+" where "+xAttribute+">="+(start-extent)+" and "+xAttribute+" <= "+(start+2*extent);
			query0 = "select min("+xAttribute+"), max("+xAttribute+"), sum("+yAttribute+"),count("+yAttribute+"),min("+yAttribute+"),max("+yAttribute+") from "+tableName+" where "+xAttribute+">="+(start-extent)+" and "+xAttribute+" <= "+(start+2*extent)+" group by "+xAttribute+" div "+factor;
			st = con.createStatement();
			long starttime = System.currentTimeMillis();
			ResultSet rs = st.executeQuery(query0);
			System.out.println(","+(System.currentTimeMillis()-starttime));
			long prevTime=0;
			while(rs.next()){
				long timed_low = rs.getLong(1);
				long timed_high = rs.getLong(2);
				long timed = (timed_high+timed_low)/2;
				double pegelSum = rs.getDouble(3);
				double pegelCnt = rs.getDouble(4);
				double pegelAvg = pegelSum/pegelCnt;
				double pegelLow = rs.getDouble(5);
				double pegelHigh = rs.getDouble(6);
				//						long timed = rs.getLong(1);
				//						double pegelAvg = rs.getDouble(3);
				//						double pegelLow = rs.getDouble(4);
				//						double pegelHigh = rs.getDouble(5);
				if(prevTime!=timed){
					obj = new Second(new Date(timed*1000));
					add(timed, pegelAvg, pegelLow, pegelHigh);
					prevTime= timed;
				} else 
					System.out.println("removed duplicate data at timestampt "+timed);
			}
			//				} 
		} catch (SQLException e) {
			e.printStackTrace();
		}
		this.fireSeriesChanged();
	}
	/**
	 * specify the start and the extent of the data 
	 * @param start
	 * @param extent
	 */
	//	public void update(long start, long extent){
	//		long factor = (long) Math.ceil(extent/MAX_RESOLUTION);
	//		long ds_factor = (long) Math.ceil(ds_extent/MAX_RESOLUTION);
	//		if (start < ds_start || start > ds_start+ds_extent || 
	//				start+extent > ds_start+ds_extent ||
	//				factor < ds_factor/2 || factor > ds_factor*2 ){
	//			System.out.print("update with start, extent, factor, querytime: "+
	//					start+","+extent+","+factor);
	//			if(factor%18049>180)
	//				System.out.println("factor not ok");
	//			else factor= factor/18049*18049;
	//			this.data.clear();			
	//			// load the data
	//			Connection con = getConnection();
	//			Object obj;
	//			if(con==null) return; 
	//			Statement st;
	//			try {
	////				if (quantile==0){
	//					// this corresponds to min and max
	//				//String query = "select "+xAttribute+", ID, avg("+yAttribute+"),min("+yAttribute+"),max("+yAttribute+") from "+tableName+" where "+xAttribute+">="+(start-extent)+" and "+xAttribute+" <= "+(start+2*extent)+" group by "+xAttribute+" div "+factor;
	//				String query;
	//				if (factor==0) 
	//					query = "select "+xAttribute+", ID, "+yAttribute+","+yAttribute+","+yAttribute+" from "+tableName+" where "+xAttribute+">="+(start-extent)+" and "+xAttribute+" <= "+(start+2*extent);
	//				else
	//					query = "select "+xAttribute+"_low,"+xAttribute+"_high,sum(vsum),sum(vcnt),min(vmin),max(vmax) from "+tableName+"_pre where "+xAttribute+"_low>="+(start-extent)+" and "+xAttribute+"_high <= "+(start+2*extent)+" group by "+xAttribute+"_low div "+factor;
	//				st = con.createStatement();
	//					long starttime = System.currentTimeMillis();
	//					ResultSet rs = st.executeQuery(query);
	//					System.out.println(","+(System.currentTimeMillis()-starttime));
	//					long prevTime=0;
	//					while(rs.next()){
	//						long timed_low = rs.getLong(1);
	//						long timed_high = rs.getLong(2);
	//						long timed = (timed_high+timed_low)/2;
	//						double pegelSum = rs.getDouble(3);
	//						double pegelCnt = rs.getDouble(4);
	//						double pegelAvg = pegelSum/pegelCnt;
	//						double pegelLow = rs.getDouble(5);
	//						double pegelHigh = rs.getDouble(6);
	////						long timed = rs.getLong(1);
	////						double pegelAvg = rs.getDouble(3);
	////						double pegelLow = rs.getDouble(4);
	////						double pegelHigh = rs.getDouble(5);
	//						if(prevTime!=timed){
	//							obj = new Second(new Date(timed));
	//							add(timed, pegelAvg, pegelLow, pegelHigh);
	//							prevTime= timed;
	//						} else 
	//							System.out.println("removed duplicate data at timestampt "+timed);
	//					}
	////				} 
	//				} catch (SQLException e) {
	//				e.printStackTrace();
	//			}
	//			this.ds_start = start-extent;
	//			this.ds_extent = start+2*extent;
	//		}
	//		this.fireSeriesChanged();
	//	}

}
