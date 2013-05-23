package nl.utwente.db.neogeo.preaggregate.mysql;


import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

public class MysqlConnectionOrder {
	private static final String CONFIG_FILENAME = "database.properties";

	private String hostname;
	private String port;
	private String username;
	private String password;
	private String database;

	public static long[][] query = {
		{1167607020,1167612060}, // pre-aggregate 4
		{1312566360,1312737480}, // pre-aggregate 6
		{1278480120,1279979820}, // pre-aggregate 8
		{1278480120,1312732140}, // pre-aggregate 10
		{1260481440,1312732140}, // pre-aggregate 10
		{1167627060,1312732140} // pre-aggregate 11
	};

	public static long[][] factorQuery = {
		{1167606600,1312737480,1410720}, // pre-aggregate 8 
		{1167607020,1167657420, 5040} // pre-aggregate 4
		};

	public static byte[] coding = {1,1,1,1,2,2,2,2,3,3,3,3,3};

	public MysqlConnectionOrder(){
		readProperties(CONFIG_FILENAME);
	}

	public Connection getConnection(){
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your Mysql JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return null;
		}
		System.out.println("MySQL JDBC Driver Registered!");
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(
					"jdbc:mysql://"+hostname+":"+port+"/"+database, username, password);
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;

		}
		if (connection != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}
		return connection;
	}

	private void readProperties(String propFilename) {
		Properties prop = new Properties();
		try {
			InputStream is =
				this.getClass().getClassLoader().getResourceAsStream(CONFIG_FILENAME);
			prop.load(is);
			hostname = prop.getProperty("hostname");
			port = prop.getProperty("port");
			username = prop.getProperty("username");
			password = prop.getProperty("password");
			database = prop.getProperty("database");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private long ipfx_d0rf(long v){
		return (v - 1167606600) / 60;
	}

	private long ipfx_d0rf_inv(long i0){
		return 1167606600 + i0*60;
	}

	private void standardQuery(Connection con, long start, long end) throws SQLException{
		String query2 = "select id from pegel_andelfingen2 B, (SELECT @vcurRow := 0) r where B.timed >= ? and timed<=? order by B.PEGEL asc;";
		PreparedStatement stmt2 = con.prepareStatement(query2);
		stmt2.setLong(1, start);
		stmt2.setLong(2, end);
		stmt2.execute();
		ResultSet rs = stmt2.getResultSet();
		int i=0;
		while(rs.next()){
			int ret = rs.getInt(1);
			//System.out.print(ret+",");
			i++;
		}
		System.out.println();
		System.out.println("size: "+i);
	}

	private void compareQueryResults(long start, long end) throws SQLException{
		Connection con = getConnection();
		String query2 = "select pegel, timed, id, @vcurRow := @vcurRow + 1 AS rank_new from pegel_andelfingen2 B, (SELECT @vcurRow := 0) r where B.timed >= ? and timed<=? order by B.PEGEL asc;";
		PreparedStatement stmt2 = con.prepareStatement(query2);
		stmt2.setLong(1, start);
		stmt2.setLong(2, end);
		stmt2.execute();
		ResultSet rs = stmt2.getResultSet();
		while(rs.next()){

		}
	}

	private void factorQuery(Connection con, long start, long end, long userFactor) throws Exception {
		PegelAndelfingen2Aggregate pegel = new PegelAndelfingen2Aggregate(con, "datagraph" , "pegel_andelfingen2", "andelfingen2");
		Vector<Long> orderV0 = new Vector<Long>();
		long s = ipfx_d0rf(start);
		long e = ipfx_d0rf(end);
		if(end-start<userFactor) userFactor=end-start;
		if(userFactor%60!=0) 
			System.out.println("userFactor is not conform with the base granularity of the pre-aggregate,w hich is 60.");
		
		Vector<OrderQuery> queries = new Vector<OrderQuery>();
		long l0 = Math.round(Math.ceil(Math.log(userFactor/60.0)/Math.log(4)));
		
		double factor = Math.pow(4, l0);
		if((factor*60)/userFactor>2)
			System.out.println("optimization possible since userFactor occurs multiple times in a factor!");
		long ss = start;
		long ee=Math.round(Math.ceil(ss / (double)userFactor)*userFactor-1);
		if((factor*0.75)<userFactor){
			if (Math.floor(ipfx_d0rf(ss) / factor) != Math.floor(ipfx_d0rf(ee) / factor)){
				System.out.println("have to increase the factor to find the right offset");
				l0++;
			}
		}
		String constr = "false";
		while(ee<end+userFactor){
			if(ee>end) ee=end;
			boolean flag = false;
			int offset = 0;
			double offsetBase = Math.pow(4, l0-1);
			while (!flag){
				flag = Math.floor((ipfx_d0rf(ss)-offset*offsetBase) / factor) == Math.floor((ipfx_d0rf(ee)-offset*offsetBase) / factor);
				if(!flag) offset++;
			}
			long v0=Math.round(Math.floor((ipfx_d0rf(ss)-offset*offsetBase) / factor)*4+offset);
			System.out.println("l0:"+l0+"     v0:"+v0);
			long st = ipfx_d0rf_inv(Math.round((v0-offset)/4*Math.round(factor)+offset*offsetBase));
			long et = ipfx_d0rf_inv(Math.round((v0+4-offset)/4*Math.round(factor)+offset*offsetBase))-1;
			if(st>ss || et<ee) 
				throw new Exception("things go wrong");
			OrderQuery q = new OrderQuery(ss, ee, l0, v0, offset, et, et);
			long soff = pegel.timeQuery("count", st, ss);
			long eoff = pegel.timeQuery("count", ee, et);
			q.setOverhead(soff, eoff);
			constr = constr + " or i0="+v0;
			queries.add(q);
			ss = ee+1;
			ee=ee+userFactor;
		}
		for(int i=0;i<queries.size();i++){
			OrderQuery q = queries.get(i);
			System.out.println((i+1)+","+q.getL0()+","+q.getI0()+","+q.getSoff()+","+q.getEoff());
		}
		//long cnt = pegel.timeQuery("count", 1167606600, 1312737480);
		String query1 = "select i0,base_id,cnt,order_map from _ipfx_level0_order_"+l0+" where l0="+l0+" and ("+constr+")";
		Statement stmt1 = con.createStatement();
		stmt1.execute(query1);
		ResultSet rs = stmt1.getResultSet();
		int i=0;
		OrderQuery q = queries.get(i);
		while(rs.next()){
			int i0 = rs.getInt("i0");
			long base_id = rs.getLong("base_id");
			int cnt = rs.getInt("cnt");
			InputStream is = rs.getBinaryStream("order_map");
			byte[] b = new byte[500000];
			int r = is.read(b, 0, cnt);
			is.close();
			while(q.getI0()==i0){
				int c = 0;
				System.out.print((c++)+"("+(cnt-q.getEoff()-q.getSoff())+"): ");
				for(int ii=(int)q.getSoff(); ii<(cnt-q.getEoff()); ii++)
					System.out.print(String.format("%02X ", b[ii]));
				System.out.println();
				i++;
				if(i>=queries.size()) break;
				q = queries.get(i);
			}
		}
	}


	private void query(Connection con, long start, long end) throws Exception {
		long s = ipfx_d0rf(start);
		long e = ipfx_d0rf(end);
		int l0 = (int) Math.round(Math.ceil(Math.log(e-s)/Math.log(4)));
		int v0;
		System.out.println("aggregation level:"+l0);
		System.out.println("difference in buckets: "+(e-s));
		boolean flag = false;
		double factor = Math.pow(4, l0);
		long offset = 0;
		double offsetBase = Math.pow(4, l0-1);
		while (!flag){
			flag = Math.floor((s-offset*offsetBase) / factor) == Math.floor((e-offset*offsetBase) / factor);
			if(!flag) offset++;
		}
		v0=(int) Math.round(Math.floor((s-offset*offsetBase) / factor)*4+offset);
		System.out.println("l0:"+l0+"     v0:"+v0);
		long st = ipfx_d0rf_inv(Math.round((v0-offset)/4*Math.round(factor)+offset*offsetBase));
		long et = ipfx_d0rf_inv(Math.round((v0+4-offset)/4*Math.round(factor)+offset*offsetBase))-1;
		if(st>start || et<end) 
			throw new Exception("things go wrong");

		String query0 = "select count(*) from pegel_andelfingen2 where timed>=? and timed<=?";
		String query1 = "select base_id,cnt,order_map from _ipfx_level0_order_"+l0+" where l0="+l0+" and i0="+v0;
		PreparedStatement stmt = con.prepareStatement(query0);
		stmt.setLong(1, st);
		stmt.setLong(2, start-1);
		stmt.execute();
		ResultSet rs = stmt.getResultSet();
		rs.next();
		int soff = rs.getInt(1);

		stmt.setLong(1, end+1);
		stmt.setLong(2, et);
		stmt.execute();
		rs = stmt.getResultSet();
		rs.next();
		int eoff = rs.getInt(1);

		Statement stmt1 = con.createStatement();
		stmt1.execute(query1);
		rs = stmt1.getResultSet();
		rs.next();
		long base_id = rs.getLong(1);
		int cnt = rs.getInt(2);
		InputStream is = rs.getBinaryStream(3);
		byte[] b = new byte[MysqlConnectionOrder.coding[l0]*(cnt+1)];
		int ret = is.read(b);
		int[] data = new int[cnt+1];
		is.read(b);
		for(int i=soff;i<cnt-eoff;i++){
			switch (MysqlConnectionOrder.coding[l0]){
			case 1: data[(b[i] & 0xFF)]= i;
			break;
			case 2: data[(b[i*2] & 0xFF)*0x100 + (b[i*2+1] & 0xFF)]= i;
			break;
			case 3: data[(b[i*3] & 0xFF)*0x10000 + (b[i*3+1] & 0xFF)*0x100 + (b[i*3+2] & 0xFF)]= i;
			break;
			case 4: data[(b[i*4] & 0xFF)*0x1000000 + (b[i*4+1] & 0xFF)*0x10000 + (b[i*4+2] & 0xFF)*0x100 + (b[i*4+3] & 0xFF)]= i;
			break;
			}
		}
		long pos = (cnt-soff-eoff)/2;
		int c=0;
		int med = -1;
		for(int i=0;i<cnt;i++){
			if(data[i]>0){
//				System.out.print(base_id+data[i]+",");
				c++;
				if(c==pos) med=data[i];
			}
		}		
		System.out.println();
		System.out.println("size: "+c);
	}

	public static void main(String[] argv) throws Exception {
		MysqlConnectionOrder psqlCon = new MysqlConnectionOrder();
		// this setting is only needed for the creation of the pre-aggregate!
		// String query_setting = "SET SESSION group_concat_max_len = 2376930*3";

		int i=1;
		long start, end;
		Connection con = psqlCon.getConnection();

		//		i=0;
		//		start = System.currentTimeMillis();
		//		psqlCon.standardQuery(con, query[i][0], query[i][1]);
		//		end = System.currentTimeMillis();
		//		System.out.println("query "+i+"   standard query time [ms]: "+(end-start));
		//
//		i=1;
//		start = System.currentTimeMillis();
//		psqlCon.standardQuery(con, query[i][0], query[i][1]);
//		end = System.currentTimeMillis();
//		System.out.println("query "+i+"   standard query time [ms]: "+(end-start));
		//		
		//		i=0;
		//		start = System.currentTimeMillis();
		//		psqlCon.query(con, query[i][0], query[i][1]);
		//		end = System.currentTimeMillis();
		//		System.out.println("query "+i+"   pre-aggregate time [ms]: "+(end-start));

//		i=1;
//		start = System.currentTimeMillis();
//		psqlCon.query(con, query[i][0], query[i][1]);
//		end = System.currentTimeMillis();
//		System.out.println("query "+i+"   pre-aggregate time [ms]: "+(end-start));

//		i=3;
//		start = System.currentTimeMillis();
//		psqlCon.query(con, query[i][0], query[i][1]);
//		end = System.currentTimeMillis();
//		System.out.println("query "+i+"   pre-aggregate time [ms]: "+(end-start));
//
//		i=3;
//		start = System.currentTimeMillis();
//		psqlCon.standardQuery(con, query[i][0], query[i][1]);
//		end = System.currentTimeMillis();
//		System.out.println("query "+i+"   standard query time [ms]: "+(end-start));

		i=1;
		start = System.currentTimeMillis();
		psqlCon.factorQuery(con, factorQuery[i][0], factorQuery[i][1], factorQuery[i][2]);
		end = System.currentTimeMillis();
		System.out.println("factorQuery "+i+"   pre-aggregate time [ms]: "+(end-start));



	}

}