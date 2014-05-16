package nl.utwente.db.named_entity_recog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import javax.json.stream.JsonGenerator;

import nl.utwente.db.neogeo.twitter.SqlUtils;
import nl.utwente.db.neogeo.utils.GeoUtils;

public class NationaalRegisterStreetDB {
	
	private static final boolean verbose = false;
	
	public static String MY_SRID = "4326";
	
	public static final String csSchema = "public";
	public static final String csTable = "nrg_street";
	
	public final int NRG_STATUS_OK = 1;
	public final int NRG_STATUS_EMPTY = 0;
	public final int NRG_STATUS_ERROR = -1;
	
	Connection c;
	
	public NationaalRegisterStreetDB(Connection c) throws SQLException {
		setConnection(c);
	}
	
	private long nStreets = -1;
	
	private PreparedStatement ps_existsStreet = null;
	private PreparedStatement ps_insertStreet = null;
	private PreparedStatement ps_selectStreet = null;
	
	public void setConnection(Connection c) throws SQLException {
		this.c = c;
		//
		if ( ! SqlUtils.existsTable(c, csSchema, csTable) )
			createTable();
		//
		nStreets = this.tableSize();
		//
		this.ps_existsStreet = c.prepareStatement(
				"select count(*)>0 from "+csSchema+"."+csTable+" where lower(street) = ?;"
		);
		this.ps_insertStreet = c.prepareStatement("INSERT INTO " + csSchema + "." + csTable + "  (" +
				// "postalcode," +
				"id," +
				"city," +
				"municipality," +
				"city_geonameid," +
				"province," +
				"street,"+
				"count_nl," +
				"status,"+ 
				"coordinates" +
		") VALUES(?,?,?,?,?,?,?,?,st_transform(ST_SetSRID(st_makepoint(?,?),28992),4326));");
		this.ps_selectStreet = c.prepareStatement(
				"select city, municipality, province, street, status, st_x(coordinates), st_y(coordinates) from "+ csSchema + "." + csTable +" where lower(street) = ?;"
			);
	}
	
	public boolean existsStreet(String street) throws SQLException {
		ps_existsStreet.setString(1,street.toLowerCase());
		ResultSet rs = ps_existsStreet.executeQuery();
		rs.next();
		boolean res = rs.getBoolean(1);
		// System.out.println("#!existsStreet("+street+")="+res);
		return res;
	}
	
	public Vector<StreetEntity> selectStreetEntity(NamedEntity streetEntity) throws SQLException {
		String street = streetEntity.getName();
		ps_selectStreet.setString(1,street.toLowerCase());
		ResultSet rs = ps_selectStreet.executeQuery();
		if ( !rs.next() ) {
			if ( verbose ) 
				System.out.println("#!selectStreet("+street+"): not in cache");
			getStreetFromNrg(street);
			return selectStreetEntity(streetEntity);
		}
		if ( verbose )
			System.out.println("#!selectStreet("+street+"): in cache");
		Vector<StreetEntity> res = new Vector<StreetEntity>();
		do {
			String city = rs.getString(1);
			String municipality = rs.getString(2);
			String province = rs.getString(3);
			// String street = rs.getString(4);
			int status = rs.getInt(5);
			double lat = rs.getDouble(6);
			double lon = rs.getDouble(7);
			if ( status == NRG_STATUS_OK ) {
				res.add(new StreetEntity(streetEntity,city,municipality,province,street,lat,lon));
			} else {
				if ( verbose )
					if ( status == NRG_STATUS_ERROR )
						System.out.println("#!selectStreet("+street+"): ERROR status");
					else
						System.out.println("#!selectStreet("+street+"): EMPTY status");
				return null;
			}
		} while (rs.next() );
		return res;
	}
	
	public void insertStreet(String city, String municipality, String province, String street, int city_geonameid, int count_nl, int status, double lat28992, double lon28992) throws SQLException {
		++nStreets;
		//
		ps_insertStreet.setLong(1,nStreets);
		ps_insertStreet.setString(2, city);
		ps_insertStreet.setString(3, municipality);
		ps_insertStreet.setInt(4, city_geonameid);
		ps_insertStreet.setString(5, province);
		ps_insertStreet.setString(6, street);
		ps_insertStreet.setInt(7, count_nl);
		ps_insertStreet.setInt(8, status);
		ps_insertStreet.setDouble(9, lat28992);
		ps_insertStreet.setDouble(10, lon28992);
		//
		ps_insertStreet.execute();
	}
	
	private long tableSize() throws SQLException {
		return SqlUtils.execute_1long(c, "select count(*) from "+csSchema+"."+csTable+";");
	}
	
	public void createTable() throws SQLException {
		if ( SqlUtils.existsTable(c, csSchema, csTable) )
			SqlUtils.dropTable(c,csSchema, csTable);
		SqlUtils.executeNORES(c, 
				"CREATE TABLE " + csSchema + "." + csTable + " (" +
				"id bigint PRIMARY KEY," +
				"city varchar(64)," + 
				"municipality varchar(64)," + // gemeente in dutch
				"province varchar(64)," + 
				"city_geonameid integer," +
				"street varchar(44)," +
				"count_nl int," +
				"status integer" + // 0 not initialized, -1 error, 1 OK, 2 ....
		");");
		SqlUtils.execute(c,
				"SELECT AddGeometryColumn('"+csSchema+"','"+csTable+"','coordinates','"+MY_SRID+"','POINT',2);");
			SqlUtils.executeNORES(c,
					"CREATE INDEX ON "+csSchema+"."+csTable+" using gist(coordinates);");
		SqlUtils.executeNORES(c,
				"create index lc_hash_nrg_city on "+csSchema+"."+csTable+" USING hash(lower(city));"
		);
		SqlUtils.executeNORES(c,
				"create index lc_hash_nrg_municipality on "+csSchema+"."+csTable+" USING hash(lower(municipality));"
		);
		SqlUtils.executeNORES(c,
				"create index lc_hash_nrg_street on "+csSchema+"."+csTable+" USING hash(lower(street));"
		);
	}
	
	private static void dropNrgTable(Connection c, String schema, String table) throws SQLException {
		try {
			SqlUtils.execute(c,
				"SELECT DropGeometryColumn('"+schema+"','"+table+"','coordinates');"); 
				// SqlUtils.dropTable(c, schema, table);
			} catch (SQLException e) {
				System.out.println("IGNORE: "+e);
			}
		SqlUtils.dropTable(c,schema,table);
	}
	
	public void getStreetFromNrg(String streetName) throws SQLException {
		getStreetFromNrg(streetName,1,false);
	}
	
	public void getStreetFromNrg(String streetName, int count_nl, boolean debug) throws SQLException {
		String nrg_str[] = GeoUtils.get_street_nrg(null,streetName);
		
		if ( (nrg_str != null) && (nrg_str.length > 0) ) {
			if ( debug )
				System.out.println("HIT: "+streetName);
			for (int i = 0; i < nrg_str.length; i++) {
				String str_split[] = nrg_str[i].split("@", 4);
				//
				String city = str_split[0];
				String municipality = str_split[1];
				String province = str_split[2];
				String str_coord = str_split[3];
				int sep_coord = str_coord.indexOf(" ");
				Double d1 = new Double(str_coord.substring(0, sep_coord));
				Double d2 = new Double(str_coord.substring(sep_coord + 1));
				this.insertStreet(city,municipality,province,streetName,-1,count_nl,NRG_STATUS_OK,d1.doubleValue(),d2.doubleValue());
			}
		} else {
			if ( nrg_str == null ) {
				if ( debug )
					System.out.println("ERROR: "+streetName);
				this.insertStreet(null,null,null,streetName,-1,count_nl,NRG_STATUS_ERROR,0.0,0.0);
			} else
				if ( debug ) {
					System.out.println("EMPTY: "+streetName);
				this.insertStreet(null,null,null,streetName,-1,count_nl,NRG_STATUS_EMPTY,0.0,0.0);
			}
		}
	}
	
	/*
	 * 
	 * 
	 */
	private void getStreets(String from) throws SQLException {
		System.out.println("#!Getting NRG streets from: " + from);
		PreparedStatement ps = c
				.prepareStatement("SELECT street, count from all_dutch_streets where street >= ? ORDER BY street;");
		ps.setString(1, from);
		ResultSet rs = ps.executeQuery();
		int webCount = 0;
		while (rs.next()) {
			boolean debug = true;
			String street = rs.getString(1);
			int count_nl = rs.getInt(2);
			System.out.println("READING: " + street + "/" + count_nl);
			// INCOMPLETE: check op & en e.v.t. -
			if (!this.existsStreet(street)) {
				if ((++webCount % 50) == 0) {
					System.out.println("SLEEP 8 sec");
					try {
						Thread.sleep(8000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				getStreetFromNrg(street, count_nl, debug);
			} else {
				if (debug)
					System.out.println("CACHED: " + street);
			}
		}
		JsonGenerator jg;
	}
	
	private final String area_cache_raw = "areac_raw_";
	private final String area_cache_gb = "areac_gb_";
	
	public void handleCacheStart(String label, int option, double sw_lat, double sw_lon, double ne_lat, double ne_lon) throws SQLException {
		String cache_rawname = area_cache_raw+label;
		String cache_gbname = area_cache_gb+label;
		
		if ( SqlUtils.existsTable(c, csSchema, cache_rawname))
			SqlUtils.dropTable(c, csSchema, cache_rawname);
		if ( SqlUtils.existsTable(c, csSchema, cache_gbname))
			SqlUtils.dropTable(c, csSchema, cache_gbname);
		String hdr = 
			"SELECT city,street AS name,1000 AS category, st_x(coordinates) AS lon,st_y(coordinates) AS lat,count_nl INTO " + csSchema + "." + cache_rawname+" " +
			"FROM "+ csSchema + "." + "nrg_street " +
			"WHERE ST_Contains(";
		String ftr =
			", coordinates" +
			");";
		PreparedStatement ps = null;
		// INCOMPLETE: if bbox
		ps = c.prepareStatement(
					hdr +
					"ST_SetSRID(" +
						"ST_MakeBox2D(" +
							"ST_Point(?,?), ST_Point(?,?)" +
						"), " +
						"4326" +
					")" +
					ftr
		    );
		ps.setDouble(1, sw_lat);
		ps.setDouble(2, sw_lon);
		ps.setDouble(3, ne_lat);
		ps.setDouble(4, ne_lon);
		
		ps.execute();
		
		ps = c.prepareStatement(
				"CREATE INDEX "+cache_rawname+"_nidx" + " ON " +  csSchema + "." + cache_rawname + " USING hash(name);"
			);
		ps.execute();
		
		//
		//
		//
		
		ps = c.prepareStatement(
				"SELECT name,count(name) AS count_area INTO " + csSchema + "." + cache_gbname +
				" FROM "+csSchema + "." + cache_rawname+" GROUP BY name;"
			 );
		ps.execute();
		
		ps = c.prepareStatement(
				"CREATE INDEX "+cache_gbname+"_nidx" + " ON " +  csSchema + "." + cache_gbname + " USING hash(name);"
			);
		ps.execute();
	}
	
	public void handleCacheEnd(String label, int option) throws SQLException {
		// SqlUtils.dropTable(c, csSchema, area_cache_raw+label);
		// SqlUtils.dropTable(c, csSchema, area_cache_gb+label);
	}
	
	public ResultSet streetBoxCount(String label, int option, double sw_lat, double sw_lon, double ne_lat, double ne_lon) throws SQLException {
		long startTime = System.currentTimeMillis();
		handleCacheStart(label,option,sw_lat,sw_lon,ne_lat,ne_lon);
		
		PreparedStatement ps = c.prepareStatement(
				"SELECT all_ent.name AS name,city,category,lon,lat,count_nl,count_area " +
				"FROM " + csSchema + "." + (area_cache_raw+label) + " AS all_ent , " + csSchema + "." + (area_cache_gb+label) + " AS gb_ent " +
				"WHERE all_ent.name = gb_ent.name;"
		    );
		// System.out.println("xx: "+ps);
		
		handleCacheEnd(label,option);
		long elapsedTime = System.currentTimeMillis() - startTime;
		if ( false ) 
			System.out.println("Elapsed time: "+elapsedTime + "ms.");
		
		return ps.executeQuery();
	}
	
	public void bboxQuery(double sw_lon, double sw_lat, double ne_lon, double ne_lat) throws SQLException {
		PreparedStatement ps = c.prepareStatement(
				"SELECT city,street,ST_X(coordinates),ST_Y(coordinates) " +
				"FROM nrg_street " +
				"WHERE ST_Contains(" + 
					"ST_SetSRID(" +
						"ST_MakeBox2D(" +
							"ST_Point(?,?), ST_Point(?,?)" +
						"), " +
						"4326" +
					"), " +
					"coordinates" +
					");"
		    );
		ps.setDouble(1, sw_lon);
		ps.setDouble(2, sw_lat);
		ps.setDouble(3, ne_lon);
		ps.setDouble(4, ne_lat);

		System.out.println("xx: "+ps);
	}
	
	
	public static void main(String[] args) {
		try {
//			if ( false ) {
//				dropNrgTable(GeoNamesDB.getConnection(), csSchema,csTable);
//				System.exit(0);
//			}
			NationaalRegisterStreetDB nrgStreetDB = new NationaalRegisterStreetDB( GeoNamesDB.geoNameDBConnection() );
			// nrgStreetDB.getStreets("");
			// Den Helder 52.9333¡ N, 4.7500¡ E
			// Maastricht: 50.8500¡ N, 5.6833¡ E
			// nrgStreetDB.streetBoxCount("xxx",0,8, 47,3,54); // totalNL
			
			nrgStreetDB.streetBoxCount("xxx",0,6.9, 52.2167, 6.95, 52.22);
			
		}	catch (SQLException e) {
			System.out.println("CAUGHT: "+e);
			e.printStackTrace();
		}
    }
	
}