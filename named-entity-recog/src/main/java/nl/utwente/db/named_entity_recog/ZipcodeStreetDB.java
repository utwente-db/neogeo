package nl.utwente.db.named_entity_recog;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Hashtable;

public class ZipcodeStreetDB
{
	public static final String csSchema = "public";
	public static final String csTable = "city_street";
	
	Connection c;
	Hashtable<String,Integer> cityGeonameCache;
	
	public ZipcodeStreetDB(Connection c) throws SQLException {
		setConnection(c);
		this.cityGeonameCache = new Hashtable<String,Integer>();
	}
	
	private PreparedStatement ps_existsStreet = null;
	
	public void setConnection(Connection c) throws SQLException {
		this.c = c;
		this.ps_existsStreet = c.prepareStatement(
				"select count(*)>0 from "+csSchema+"."+csTable+" where lower(street) = ?;"
		);
	}
	
	public void buildFromFile(String fileName) throws IOException, SQLException {
		InputStream    fis;
		BufferedReader br;
		String         line;

		buildTable();
		HashSet sofar = new HashSet<String>();
		//
		fis = new FileInputStream(fileName);
		br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
		int cnt = 0, found = 0;
		PreparedStatement ps = c.prepareStatement("INSERT INTO " + csSchema + "." + csTable + "  (" +
				// "postalcode," +
				"id," +
				"city," +
				"city_geonameid," +
				"street,"+
				"status,"+ 
				"latitude," +
				"longitude" +
		") VALUES(?,?,?,?,?,?,?);");
		while ((line = br.readLine()) != null) {
		    // Deal with the line
			String postcode = line.substring(0, 6);
			String city = line.substring(35, 35+24).trim();
			String street = line.substring(100, 100+43).trim();
			String cs = city+"/"+street;
			double lat = 0.0;
			double lon = 0.0;
			if ( isStreetName(street) && !sofar.contains(cs) ) {
				found++;
				int city_geonameid = findGeonameCity(city);
				ps.setInt(1, found);
				ps.setString(2, city);
				ps.setInt(3, city_geonameid);
				ps.setString(4, street);
				ps.setInt(5, 0);
				ps.setDouble(6, lat);
				ps.setDouble(7, lon);
				ps.execute();
				//
				sofar.add(cs);
				// System.out.println("["+postcode+"/"+city+"/"+street+"]");
			}
			++cnt;
//			if ( ++cnt > 20000 )
//				break;
		}
		System.out.println("Handled: "+cnt+" postal codes, found "+found+" streets"); // 253K in db

		// Done with the file
		br.close();
	}
	
	private boolean isStreetName(String name) {
		if ( name.toLowerCase().equals("postbus") )
			return false;
		return true;
	}
	
	public boolean existsStreet(String name) throws SQLException {
		ps_existsStreet.setString(1,name.toLowerCase());
		ResultSet rs = ps_existsStreet.executeQuery();
		rs.next();
		boolean res = rs.getBoolean(1);
		// System.out.println("#!existsStreet("+name+")="+res);
		return res;
	}
	
	private int findGeonameCity(String name) throws SQLException {
		Integer id = this.cityGeonameCache.get(name);
		
		if ( id != null )
			return id.intValue();
		ps_existsStreet.setString(1,name);
		ResultSet rs = ps_existsStreet.executeQuery();
		if ( rs.next() ) {
			int fid = rs.getInt(1);
			System.out.println("Geonameid for \""+name+"\"="+fid);
			this.cityGeonameCache.put(name,new Integer(fid));
			if ( rs.next() )
				System.out.println("EXTRA RESULT FOR: "+name);
			return fid;
		}
		System.out.println("FAIL: "+name);
		return 0;
	}
	
	public void buildTable() throws SQLException {
		if ( SqlUtils.existsTable(c, csSchema, csTable) )
			SqlUtils.dropTable(c,csSchema, csTable);
		SqlUtils.executeNORES(c, 
				"CREATE TABLE " + csSchema + "." + csTable + " (" +
				"id integer PRIMARY KEY," +
				"city varchar(25)," + 
				"city_geonameid integer," +
				"street varchar(44)," +
				"status integer," + // 0 not initialized, -1 error, 1 OK, 2 ....
				"latitude double precision,"+
				"longitude double precision"+
		");");
		SqlUtils.executeNORES(c,
				"create index lc_hash_city on "+csSchema+"."+csTable+" USING hash(lower(city));"
		);
		SqlUtils.executeNORES(c,
				"create index lc_hash_street on "+csSchema+"."+csTable+" USING hash(lower(street));"
		);
	}
	
	public static void main(String[] args)
    {
		try {
			ZipcodeStreetDB zipStreetDB = new ZipcodeStreetDB( GeoNamesDB.geoNameDBConnection() );
			// zipStreetDB.buildFromFile("/Users/flokstra/Downloads/Goodies/postcodes_nl.txt");
			zipStreetDB.existsStreet("Reelaan");
		} catch (Exception e) {
			System.out.println("#CAUGHT: "+e);
			e.printStackTrace();
		}
    }
}