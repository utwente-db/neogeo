package nl.utwente.db.named_entity_recog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import nl.utwente.db.neogeo.twitter.SqlUtils;

public class GeoNameEntityTable {
	
	private static final boolean verbose = true;
	
	public static String MY_SRID = "4326";
	
	public static final String geoSchema = "public";
	public static final String geoentityTable = "geoentity";
	public static final String categoryTable = "geoentity_category";

	
	Connection c;
	
	public GeoNameEntityTable(Connection c) throws SQLException {
		setConnection(c);
	}
	
	private long nEntities = -1;
	
	private PreparedStatement ps_existsEntity = null;
	private PreparedStatement ps_insertEntity = null;
	private PreparedStatement ps_selectEntity = null;
	private PreparedStatement ps_selectCategory_by_id = null;
	private PreparedStatement ps_selectCategory_by_name = null;
	private PreparedStatement ps_insertCategory = null;
	
	public void setConnection(Connection c) throws SQLException {
		this.c = c;
		//		//
		if ( ! SqlUtils.existsTable(c, geoSchema, geoentityTable) )
			createTable();
		//
		nEntities = this.tableSize();
		//
		this.ps_existsEntity = c.prepareStatement(
				"select count(*)>0 from "+geoSchema+"."+geoentityTable+" where lower(name) = ?;"
		);
		this.ps_insertEntity = c.prepareStatement("INSERT INTO " + geoSchema + "." + geoentityTable + "  (" +
				// "postalcode," +
				"id," +				
				"name," +
				"category," +
				"category_str," +
				"city," +
				"province," +
				"extra,"+
				"count_nl," +
				"coordinates" +
		") VALUES(?,?,?,?,?,?,?,?,st_transform(ST_SetSRID(st_makepoint(?,?),4326),4326));");
		this.ps_selectEntity = c.prepareStatement(
				"select id, name, category, category_str, city, province, extra, st_x(coordinates), st_y(coordinates) from "+ geoSchema + "." + geoentityTable +" where lower(name) = ?;"
			);
		//
		this.ps_selectCategory_by_id = c.prepareStatement(
				"select name from "+ geoSchema + "." + categoryTable +" where id = ?;"
			);
		this.ps_selectCategory_by_name = c.prepareStatement(
				"select id from "+ geoSchema + "." + categoryTable +" where lower(name) = ?;"
			);
		this.ps_insertCategory = c.prepareStatement("INSERT INTO " + geoSchema + "." + categoryTable + "  (" +
					"id," +				
					"name" +
				") VALUES(?,?);");
	}
	
	public void defineCategory(int id, String name) throws SQLException {
		if ( getCategory(id) == null ) {
			ps_insertCategory.setInt(1,id);
			ps_insertCategory.setString(2,name);
			//
			ps_insertCategory.execute();
			
		}
	}
	
	public String getCategory(int id) throws SQLException {
		ps_selectCategory_by_id.setInt(1, id);
		ResultSet rs = ps_selectCategory_by_id.executeQuery();
		if ( rs.next() ) {
			return rs.getString(1);
		}
		return null;
	}
	
	public int getCategory(String name) throws SQLException {
		ps_selectCategory_by_name.setString(1, name.toLowerCase());
		ResultSet rs = ps_selectCategory_by_name.executeQuery();
		if ( rs.next() ) {
			return rs.getInt(1);
		}
		return 0;
	}
	
	private static int isNumericCategory(String s) {
		try {
			return new Integer(s).intValue();
		} catch (Exception e) {
			return -1;
		}
	}
	
	public boolean allSelectCategory(Vector<Integer> v) {
		return ( v.size() > 0) &&  (v.get(0) == Integer.MAX_VALUE );
	}
	
	public void set_allSelectCategory(Vector<Integer> v) {
		v.clear();
		v.add(Integer.MAX_VALUE);
	}
	
	public String handleSelectCategory(String name, Vector<Integer> v) throws SQLException {
		if ( !allSelectCategory(v) ) {
			if ( name.toLowerCase().equals("all")) {
				set_allSelectCategory(v);
			} else {
				// incomplete
				int id = this.getCategory(name);
				
				if ( id > 0 )
					v.add(new Integer(id));
				else {
					id = isNumericCategory(name);
					
					if ( id > 0 )
						v.add(new Integer(id));
					else {
						System.out.println("UNKNOW CATEGORY: "+name);
						return "UNKNOW CATEGORY: "+name;
					}
				}
			}
		}
		return null;
	}
	
	public boolean existsEntity(String name) throws SQLException {
		ps_existsEntity.setString(1,name.toLowerCase());
		ResultSet rs = ps_existsEntity.executeQuery();
		rs.next();
		boolean res = rs.getBoolean(1);
		// System.out.println("#!existsEntity("+name+")="+res);
		return res;
	}
	
//	public Vector<StreetEntity> selectStreetEntity(NamedEntity streetEntity) throws SQLException {
//		String street = streetEntity.getName();
//		ps_selectStreet.setString(1,street.toLowerCase());
//		ResultSet rs = ps_selectStreet.executeQuery();
//		if ( !rs.next() ) {
//			if ( verbose ) 
//				System.out.println("#!selectStreet("+street+"): not in cache");
//			getStreetFromNrg(street);
//			return selectStreetEntity(streetEntity);
//		}
//		if ( verbose )
//			System.out.println("#!selectStreet("+street+"): in cache");
//		Vector<StreetEntity> res = new Vector<StreetEntity>();
//		do {
//			String city = rs.getString(1);
//			String municipality = rs.getString(2);
//			String province = rs.getString(3);
//			// String street = rs.getString(4);
//			int status = rs.getInt(5);
//			double lat = rs.getDouble(6);
//			double lon = rs.getDouble(7);
//			if ( status == NRG_STATUS_OK ) {
//				res.add(new StreetEntity(streetEntity,city,municipality,province,street,lat,lon));
//			} else {
//				if ( verbose )
//					if ( status == NRG_STATUS_ERROR )
//						System.out.println("#!selectStreet("+street+"): ERROR status");
//					else
//						System.out.println("#!selectStreet("+street+"): EMPTY status");
//				return null;
//			}
//		} while (rs.next() );
//		return res;
//	}
	
	public void insertEntity(String name, int category, String category_str, String city, String province, String extra, int count_nl, double lat28992, double lon28992) throws SQLException {
		++nEntities;
		//
		ps_insertEntity.setLong(1,nEntities);
		ps_insertEntity.setString(2,name);
		ps_insertEntity.setInt(3,category);
		ps_insertEntity.setString(4,category_str);
		ps_insertEntity.setString(5, city);
		ps_insertEntity.setString(6, province);
		ps_insertEntity.setString(7, extra);
		ps_insertEntity.setInt(8, count_nl);
		ps_insertEntity.setDouble(9, lat28992);
		ps_insertEntity.setDouble(10, lon28992);
		//
		ps_insertEntity.execute();
	}
	
	private long tableSize() throws SQLException {
		return SqlUtils.execute_1long(c, "select count(*) from "+geoSchema+"."+geoentityTable+";");
	}
	
	public void createTable() throws SQLException {
		if ( SqlUtils.existsTable(c, geoSchema, geoentityTable) )
			SqlUtils.dropTable(c,geoSchema, geoentityTable);
		SqlUtils.executeNORES(c, 
				"CREATE TABLE " + geoSchema + "." + geoentityTable + " (" +
				"id bigint PRIMARY KEY," +
				"name text," +
				"category int," +
				"category_str text," +
				"city varchar(64)," + 
				"province varchar(64)," + 
				"extra text," +
				"count_nl int" +
		");");
		SqlUtils.execute(c,
				"SELECT AddGeometryColumn('"+geoSchema+"','"+geoentityTable+"','coordinates','"+MY_SRID+"','POINT',2);");
			SqlUtils.executeNORES(c,
					"CREATE INDEX ON "+geoSchema+"."+geoentityTable+" using gist(coordinates);");
		SqlUtils.executeNORES(c,
				"create index lc_hash_tt_city on "+geoSchema+"."+geoentityTable+" USING hash(lower(city));"
		);
		SqlUtils.executeNORES(c,
				"create index lc_hash_tt_name on "+geoSchema+"."+geoentityTable+" USING hash(lower(name));"
		);
		//
		if ( SqlUtils.existsTable(c, geoSchema, categoryTable) )
			SqlUtils.dropTable(c,geoSchema, categoryTable);
		SqlUtils.executeNORES(c, 
				"CREATE TABLE " + geoSchema + "." + categoryTable + " (" +
				"id int PRIMARY KEY," +
				"name text" +
		");");
		this.defineCategory(1000, "Streets");
	}
	
	public void xdropTables() throws SQLException {
		dropTable(c, geoSchema,geoentityTable);
		SqlUtils.dropTable(c, geoSchema,categoryTable);
	}
	
	private static void dropTable(Connection c, String schema, String table) throws SQLException {
		try {
			SqlUtils.execute(c,
				"SELECT DropGeometryColumn('"+schema+"','"+table+"','coordinates');"); 
			} catch (SQLException e) {
				System.out.println("IGNORE: "+e);
			}
		SqlUtils.dropTable(c,schema,table);
	}
	
//	/*
//	 * 
//	 * 
//	 */
//	private void getStreets(String from) throws SQLException {
//		System.out.println("#!Getting NRG streets from: " + from);
//		PreparedStatement ps = c
//				.prepareStatement("SELECT street, count from all_dutch_streets where street >= ? ORDER BY street;");
//		ps.setString(1, from);
//		ResultSet rs = ps.executeQuery();
//		int webCount = 0;
//		while (rs.next()) {
//			boolean debug = true;
//			String street = rs.getString(1);
//			int count_nl = rs.getInt(2);
//			System.out.println("READING: " + street + "/" + count_nl);
//			// INCOMPLETE: check op & en e.v.t. -
//			if (!this.existsStreet(street)) {
//				if ((++webCount % 50) == 0) {
//					System.out.println("SLEEP 8 sec");
//					try {
//						Thread.sleep(8000);
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//				getStreetFromNrg(street, count_nl, debug);
//			} else {
//				if (debug)
//					System.out.println("CACHED: " + street);
//			}
//		}
//		JsonGenerator jg;
//	}
	
	private final String area_cache_raw = "areac_raw_";
	private final String area_cache_gb = "areac_gb_";
	
	public void handleCacheStart(String label, int option, double sw_lat, double sw_lon, double ne_lat, double ne_lon) throws SQLException {
		String cache_rawname = area_cache_raw+label;
		String cache_gbname = area_cache_gb+label;
		
		if ( SqlUtils.existsTable(c, geoSchema, cache_rawname))
			SqlUtils.dropTable(c, geoSchema, cache_rawname);
		if ( SqlUtils.existsTable(c, geoSchema, cache_gbname))
			SqlUtils.dropTable(c, geoSchema, cache_gbname);
		String hdr = 
			"SELECT city, name, category, category_str, st_x(coordinates) AS lon,st_y(coordinates) AS lat,count_nl INTO " + geoSchema + "." + cache_rawname+" " +
			"FROM "+ geoSchema + "." + "nrg_street " +
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
				"CREATE INDEX "+cache_rawname+"_nidx" + " ON " +  geoSchema + "." + cache_rawname + " USING hash(name);"
			);
		ps.execute();
		
		//
		//
		//
		
		ps = c.prepareStatement(
				"SELECT name,count(name) AS count_area INTO " + geoSchema + "." + cache_gbname +
				" FROM "+geoSchema + "." + cache_rawname+" GROUP BY name;"
			 );
		ps.execute();
		
		ps = c.prepareStatement(
				"CREATE INDEX "+cache_gbname+"_nidx" + " ON " +  geoSchema + "." + cache_gbname + " USING hash(name);"
			);
		ps.execute();
	}
	
	public void handleCacheEnd(String label, int option) throws SQLException {
		// SqlUtils.dropTable(c, geoSchema, area_cache_raw+label);
		// SqlUtils.dropTable(c, geoSchema, area_cache_gb+label);
	}
	
	public ResultSet streetBoxCount(String label, int option, double sw_lat, double sw_lon, double ne_lat, double ne_lon) throws SQLException {
		long startTime = System.currentTimeMillis();
		handleCacheStart(label,option,sw_lat,sw_lon,ne_lat,ne_lon);
		
		PreparedStatement ps = c.prepareStatement(
				"SELECT all_ent.name AS name,city,category,category_str,lon,lat,count_nl,count_area " +
				"FROM " + geoSchema + "." + (area_cache_raw+label) + " AS all_ent , " + geoSchema + "." + (area_cache_gb+label) + " AS gb_ent " +
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
	
}