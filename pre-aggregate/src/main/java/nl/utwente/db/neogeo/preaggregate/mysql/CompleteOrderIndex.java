package nl.utwente.db.neogeo.preaggregate.mysql;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
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

public class CompleteOrderIndex {
	private static final String CONFIG_FILENAME = "database.properties";

	private String hostname;
	private String port;
	private String username;
	private String password;
	private String database;

	public static byte[] coding = {1,1,1,1,2,2,2,2,3,3,3,3,3};

	public CompleteOrderIndex(){
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

	private void completeIndex(Connection con, int level) throws SQLException, IOException{
		String blobext = "";
		switch (MysqlConnectionOrder.coding[level]){ 
		case 1: 
			blobext = "tiny";
			break;
		case 3: 
			blobext = "medium";
			break;
		case 4: 
			blobext = "long";
			break;
		}
		String query0 = "select i0,cnt,order_map from pegel_andelfingen2_pa_order_"+blobext+"blob where l0="+level+";";
		String query1 = "update pegel_andelfingen2_pa_order_"+blobext+"blob set median_start=? ,median_end=? where l0=? and i0=?;";
		PreparedStatement stmt1 = con.prepareStatement(query1);
		stmt1.setInt(3, level);
		Statement stmt0 = con.createStatement();
		stmt0.execute(query0);
		ResultSet rs = stmt0.getResultSet();
		int i=0;
		while(rs.next()){
			int i0 = rs.getInt("i0");
			int cnt = rs.getInt("cnt");
			byte[] buf = new byte[cnt*MysqlConnectionOrder.coding[level]];
			InputStream is = rs.getBinaryStream("order_map");
			is.read(buf, 0, cnt*CompleteOrderIndex.coding[level]);

			int[] data = new int[cnt];
			for(int ii=0;ii<cnt;ii++){
				switch (MysqlConnectionOrder.coding[level]){
				case 1: data[ii]=(buf[ii] & 0xFF);
				break;
				case 2: data[ii]=(buf[ii*2] & 0xFF)*0x100 + (buf[ii*2+1] & 0xFF);
				break;
				case 3: data[ii]=(buf[ii*3] & 0xFF)*0x10000 + (buf[ii*3+1] & 0xFF)*0x100 + (buf[ii*3+2] & 0xFF);
				break;
				case 4: data[ii]=(buf[ii*4] & 0xFF)*0x1000000 + (buf[ii*4+1] & 0xFF)*0x10000 + (buf[ii*4+2] & 0xFF)*0x100 + (buf[ii*4+3] & 0xFF);
				break;
				}
			}
			byte[] start_buf = new byte[cnt*MysqlConnectionOrder.coding[level]];
			byte[] end_buf = new byte[cnt*MysqlConnectionOrder.coding[level]];
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
				switch (MysqlConnectionOrder.coding[level]){
				case 1: 
					start_buf[ii]=(byte) (start_cnt & 0xFF);
					end_buf[cnt-1-ii]=(byte) (end_cnt & 0xFF);
					break;
				case 2:
					start_buf[2*ii]=(byte) (start_cnt & 0xFF00);
					start_buf[2*ii+1]=(byte) (start_cnt & 0xFF);
					end_buf[2*(cnt-1-ii)]=(byte) (end_cnt & 0xFF00);
					end_buf[2*(cnt-1-ii)+1]=(byte) (end_cnt & 0xFF);
					break;
				case 3: 
					if (start_cnt<0) start_buf[3*ii]=(byte) 0x80;
					start_buf[3*ii]=(byte) (start_buf[3*ii] |(byte)(Math.abs(start_cnt) &  0xFF0000));
					start_buf[3*ii+1]=(byte) (Math.abs(start_cnt) & 0xFF00);
					start_buf[3*ii+2]=(byte) (Math.abs(start_cnt) & 0xFF);
					if (end_cnt<0) end_buf[3*ii]=(byte) 0x80;
					end_buf[3*ii]=(byte) (end_buf[3*ii] |(byte)(Math.abs(end_cnt) &  0xFF0000));
					end_buf[3*ii+1]=(byte) (Math.abs(end_cnt) & 0xFF00);
					end_buf[3*ii+2]=(byte) (Math.abs(end_cnt) & 0xFF);
					break;
				case 4: 
					start_buf[4*ii]=(byte) (start_cnt & 0xFF000000);
					start_buf[4*ii+1]=(byte) (start_cnt & 0xFF0000);
					start_buf[4*ii+2]=(byte) (start_cnt & 0xFF00);
					start_buf[4*ii+3]=(byte) (start_cnt & 0xFF);
					end_buf[4*(cnt-1-ii)]=(byte) (end_cnt & 0xFF000000);
					end_buf[4*(cnt-1-ii)+1]=(byte) (end_cnt & 0xFF0000);
					end_buf[4*(cnt-1-ii)+2]=(byte) (end_cnt & 0xFF00);
					end_buf[4*(cnt-1-ii)+3]=(byte) (end_cnt & 0xFF);
					break;
				}

			}
			InputStream is_start = new ByteArrayInputStream(start_buf);
			InputStream is_end = new ByteArrayInputStream(end_buf);
			stmt1.setBinaryStream(1, is_start, start_buf.length);
			stmt1.setBinaryStream(2, is_end, end_buf.length);
			stmt1.setInt(4, i0);
			stmt1.executeUpdate();
			is_start.close();
			is_end.close();
		}
	}


	public static void main(String[] argv) throws Exception {
		CompleteOrderIndex psqlCon = new CompleteOrderIndex();
		// this setting is only needed for the creation of the pre-aggregate!
		// String query_setting = "SET SESSION group_concat_max_len = 2376930*3";

		Connection con = psqlCon.getConnection();
//		for(int i=5;i<8;i++){
			psqlCon.completeIndex(con, 4);
//	}
	}

}