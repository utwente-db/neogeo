package nl.utwente.db.neogeo.preaggregate.mysql;


import java.io.ByteArrayInputStream;
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

import nl.utwente.db.neogeo.preaggregate.AggregateAxis;

public class MysqlConnectionOrder {
	private static final String CONFIG_FILENAME = "database.properties";
	public static final int  	DFLT_TIME = 60; // corresponds to ten minutes
	public static final short	DFLT_N = 4;

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
		{1167627060,1312732140}, // pre-aggregate 11
		{1167606600,1167606780}  // query only contains 4 values
	};

	public static long[][] factorQuery = {
		{1167606600,1312737480,1410720}, // pre-aggregate 8 
		{1167607020,1167657420, 5040} // pre-aggregate 4
	};

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

	private void completeIndex(Connection con) throws SQLException, IOException{
		String query0 = "select base_id, cnt, order_map from pegel_andelfingen2_pa_order;";
		String query1 = "update pegel_andelfingen2_pa_order set median_start=? ,median_end=?;";
		PreparedStatement stmt1 = con.prepareStatement(query1);
		Statement stmt0 = con.createStatement();
		stmt0.execute(query0);
		ResultSet rs = stmt0.getResultSet();
		int i=0;
		rs.next();
		long base_id = rs.getLong("base_id");
		int cnt = rs.getInt("cnt");
		byte[] buf = new byte[cnt*4];
		InputStream is = rs.getBinaryStream("order_map");
		is.read(buf, 0, cnt*4);

		int[] data = new int[cnt];
		for(int ii=0;ii<cnt;ii++){
			data[ii]=(buf[ii*4] & 0xFF)*0x1000000 + (buf[ii*4+1] & 0xFF)*0x10000 + (buf[ii*4+2] & 0xFF)*0x100 + (buf[ii*4+3] & 0xFF);
		}
		byte[] start_buf = new byte[cnt*4];
		byte[] end_buf = new byte[cnt*4];
		int start_cnt = 0;
		int end_cnt = 0;
		int median = cnt/2;
		for(int ii=0;ii<cnt;ii++){
			if(data[ii]>median) start_cnt++;
			else if(data[ii]<median) start_cnt--;
			// otherwise the start_cnt is not changing.
			if(data[cnt-1-ii]>median) end_cnt--;
			else if(data[cnt-1-ii]<median) end_cnt++;
			// otherwise the start_cnt is not changing.
			start_buf[4*ii]= (byte) ((byte) (Math.abs(start_cnt) & 0x7F000000) | (start_cnt<0 ? 0x80 : 0));
			start_buf[4*ii+1]=(byte) (Math.abs(start_cnt) & 0xFF0000);
			start_buf[4*ii+2]=(byte) (Math.abs(start_cnt) & 0xFF00);
			start_buf[4*ii+3]=(byte) (Math.abs(start_cnt) & 0xFF);
			end_buf[4*(cnt-1-ii)]=(byte) ((byte) (Math.abs(end_cnt) & 0x7F000000)| (start_cnt<0 ? 0x80 : 0));
			end_buf[4*(cnt-1-ii)+1]=(byte) (Math.abs(end_cnt) & 0xFF0000);
			end_buf[4*(cnt-1-ii)+2]=(byte) (Math.abs(end_cnt) & 0xFF00);
			end_buf[4*(cnt-1-ii)+3]=(byte) (Math.abs(end_cnt) & 0xFF);
		}
		// debug code to check an assertion
		int a,b,sa,sb;
		int max = 469763;
		for(int ii=0;ii<cnt;ii++){
			sa = (start_buf[4*ii]&0x80)>0 ?-1 :1 ;
			sb = (end_buf[4*ii]&0x80)>0 ?-1 :1 ;
			a = (start_buf[4*ii]&0x7F)*0x1000000 +
				start_buf[4*ii+1]*0x10000+start_buf[4*ii+2]*0x100+start_buf[4*ii+3];
			b = (end_buf[4*ii]&0x7F)*0x1000000 +
			end_buf[4*ii+1]*0x10000+end_buf[4*ii+2]*0x100+end_buf[4*ii+3];
			assert(max - sa*a == sb*b);
		}
		InputStream is_start = new ByteArrayInputStream(start_buf);
		InputStream is_end = new ByteArrayInputStream(end_buf);
		stmt1.setBinaryStream(1, is_start, start_buf.length);
		stmt1.setBinaryStream(2, is_end, end_buf.length);
		System.out.println(end_buf.length);
		stmt1.executeUpdate();
		is_start.close();
		is_end.close();
	}

	private String createOrderPreAggregateStart(Connection c, String schema,
			String table, String label, AggregateAxis axis[],
			int aggregates) 
	throws SQLException {
		StringBuffer sb = new StringBuffer();
		sb.append(MysqlOrderQueryBuilder.getLongBlobToIntFunctionDefinition());
		sb.append(MysqlOrderQueryBuilder.getPAOrderTable(aggregates));
		long cnt = SqlUtils.count(c, schema, table, "*");
		sb.append(MysqlOrderQueryBuilder.fillPreaggregateTable(cnt));
		return sb.toString();
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
//				int si = (b[i*4] & 0x80)>0 ? -1 : 1;
//				data[i]=si*((b[i*4] & 0x7F)*0x1000000 + (b[i*4+1] & 0xFF)*0x10000 + (b[i*4+2] & 0xFF)*0x100 + (b[i*4+3] & 0xFF));

			}
		}
	}


	private void query(Connection con, long start, long end) throws Exception {
		long s = ipfx_d0rf(start);
		long e = ipfx_d0rf(end);
		
		long start_time = System.currentTimeMillis();
		
		String query0 = "select count(*) from pegel_andelfingen2 where timed<";
		//String query2 = "select count(*) from pegel_andelfingen2";
		String query1 = "select base_id,cnt,substring(order_map,?,?), substring(order_map,-4) from pegel_andelfingen2_pa_order";
		
		int soff = SqlUtils.execute_1int(con, query0+start);
		int eoff = SqlUtils.execute_1int(con, query0+end);
		//int cnt = SqlUtils.execute_1int(con, query3);
		System.out.println("count query: "+(System.currentTimeMillis()-start_time));
		
		long start_time2 = System.currentTimeMillis();
		
		PreparedStatement stmt1 = con.prepareStatement(query1);
		stmt1.setInt(1, 4*soff+1);
		stmt1.setInt(2, 4*(eoff-soff));
		stmt1.execute();
		ResultSet rs = stmt1.getResultSet();
		rs.next();
		long base_id = rs.getLong(1);
		int cnt = rs.getInt(2);
		InputStream is = rs.getBinaryStream(3);
		byte[] b = new byte[4*(eoff-soff)];
		int ret = is.read(b);
		System.out.println("median query: "+(System.currentTimeMillis()-start_time2));
		long start_time4 = System.currentTimeMillis();
		
		int[] data = new int[cnt+1];
		for(int i=0;i<eoff-soff;i++){
			data[((b[i*4] & 0xFF)*0x1000000 + (b[i*4+1] & 0xFF)*0x10000 + (b[i*4+2] & 0xFF)*0x100 + (b[i*4+3] & 0xFF))]=soff+i;
		}
		System.out.println("parsing the binatry: "+(System.currentTimeMillis()-start_time4));
		
		int pos = (eoff-soff)/2;
		int c=0;
		int med = -1;
		long start_time3 = System.currentTimeMillis();
		for(int i=0;i<cnt+1;i++){
			if(data[i]>0){
				//				System.out.print(base_id+data[i]+",");
				c++;
				if(c==pos) med=data[i];
			}
		}		
		System.out.println("calculate median"+(System.currentTimeMillis()-start_time3));
		System.out.println("median: "+med);
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

				i=6;
				start = System.currentTimeMillis();
				psqlCon.query(con, query[i][0], query[i][1]);
				end = System.currentTimeMillis();
				System.out.println("query "+i+"   pre-aggregate time [ms]: "+(end-start));
		//
		//		i=3;
		//		start = System.currentTimeMillis();
		//		psqlCon.standardQuery(con, query[i][0], query[i][1]);
		//		end = System.currentTimeMillis();
		//		System.out.println("query "+i+"   standard query time [ms]: "+(end-start));

//				i=1;
//				start = System.currentTimeMillis();
//				psqlCon.factorQuery(con, factorQuery[i][0], factorQuery[i][1], factorQuery[i][2]);
//				end = System.currentTimeMillis();
//				System.out.println("factorQuery "+i+"   pre-aggregate time [ms]: "+(end-start));
//		AggregateAxis axis[] = {
//				new AggregateAxis("timed","long",DFLT_TIME ,DFLT_N)
//		};
//		String txt = psqlCon.createOrderPreAggregateStart(con, "datagraph" , "pegel_andelfingen2", "andelfingen2", axis,MysqlOrderQueryBuilder.MEDIAN);
//		System.out.println(txt);
//		Statement stmt = con.createStatement();
//		stmt.addBatch(txt);
//		
//		psqlCon.completeIndex(con);
	}

}