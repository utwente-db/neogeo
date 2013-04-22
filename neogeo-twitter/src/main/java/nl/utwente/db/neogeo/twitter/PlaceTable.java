package nl.utwente.db.neogeo.twitter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

import nl.utwente.db.neogeo.utils.WebUtils;

public class PlaceTable {

	public static final String GLOBAL_PLACES = "global_places";
	
	private static boolean blockedByTwitter = false;
	
	// incomplete
	// put it all in one table, for all places
	// incomplete, mark error 400 places
	private class PlaceRecord {
		
		public long id;
		public long count;
		
		public PlaceRecord(long id) {
			this.id = id;
			this.count = 0;
		}
		
		public void inc() {
			count++;
		}
	}
	
	private Connection c;
	private String schema;
	private String table;
	private long long_id = 0;
	private PreparedStatement insert_stat = null;
	
	private HashMap<String,PlaceRecord> hashmap = new HashMap<String, PlaceRecord>();	
	
	public PlaceTable(Connection c, String schema, String table, boolean isNew)
			throws SQLException {
		this.c = c;
		this.schema = schema;
		this.table = table;
		openGlobalPlaces(c,schema,GLOBAL_PLACES);
		if (isNew) {
			if (SqlUtils.existsTable(c, schema, table)) {
				try {
					// should use DropGeometryTable here
					SqlUtils.execute(c, "SELECT DropGeometryColumn('" + schema
							+ "','" + table + "','bbox');");
				} catch (SQLException e) {
					System.out.println("IGNORE: " + e);
				}
				SqlUtils.dropTable(c, schema, table);
			}
		} else 
			throw new RuntimeException("Handling existing table not impl.");
		SqlUtils.executeNORES(c,
				"CREATE TABLE " + schema +"." + table + " (" +
						"id bigint NOT NULL PRIMARY KEY," +
						"full_name text," +
						"ntweets integer," +
						"type text," +
						"street_address text," +
						"country text," +
						"country_code text," +
						"name text," +
						"id_str text," +
						"url text" +
				");"
		);
		SqlUtils.execute(c,
				"SELECT AddGeometryColumn('"+schema+"','"+table+"','bbox','"+Twitter.TWITTER_SRID+"','POLYGON',2);"
		);

		insert_stat = c.prepareStatement("INSERT INTO "
				 + schema + "." + table  + "  (id, full_name, bbox, id_str, country_code, country, type, name, street_address, url, ntweets) " + 
				 "VALUES" + "  (?, ?, ST_Polygon(ST_GeomFromText(?),"+Twitter.TWITTER_SRID+"), ?, ?, ?, ?, ?, ?, ?, ?);");
	}
	
	public long insertPlace(Tweet tweet) throws SQLException {
		String full_name = tweet.place_full_name();
		PlaceRecord pr = hashmap.get(full_name);
		if ( pr == null ) {
			pr = new PlaceRecord(++long_id);
			hashmap.put(full_name,pr);
			//
			String area = getAreaFromUrl(tweet.place_url(),tweet.place_full_name());
			if ( area == null )
				area = tweet.place_bbox();
			//
			insert_stat.setLong(1, pr.id);
			insert_stat.setString(2, full_name);
			insert_stat.setString(3, area);
			insert_stat.setString(4, tweet.place_id());
			insert_stat.setString(5, tweet.place_country_code());
			insert_stat.setString(6, tweet.place_country());
			insert_stat.setString(7, tweet.place_type());
			insert_stat.setString(8, tweet.place_name());
			insert_stat.setString(9, tweet.place_street_address());
			insert_stat.setString(10,tweet.place_url());
			insert_stat.setInt(   11,1);
			//
			insert_stat.executeUpdate();
		}
		pr.inc();
		return pr.id;
	}
	
	public void endInsert() throws SQLException {
		closeGlobalPlaces();
		insert_stat.close();
		SqlUtils.create_index(c,schema,table,"GIST","bbox");
		
		PreparedStatement stat = c.prepareStatement(
				"UPDATE "+schema+"."+table+" SET ntweets=? WHERE id = ?;");
		Iterator<PlaceRecord> it = hashmap.values().iterator();
		while ( it.hasNext() ) {
			PlaceRecord pr = it.next();
			// System.out.println("MODIFY CARD("+pr.id.longValue() +")="+pr.count);
			if ( pr.count > 1 )	{
				stat.setLong(1, pr.count);
				stat.setLong(2, pr.id);
				stat.executeUpdate();
			}
		}
	}
	
	/*
	 * 
	 * 
	 */
	private String global_schema = null;
	private String global_table = null;
	private PreparedStatement global_insert_stat = null;
	private PreparedStatement global_insert_null_stat = null;
	private PreparedStatement global_select_stat = null;
	
	private void openGlobalPlaces(Connection c, String global_schema,
			String global_table) throws SQLException {
		this.global_schema = global_schema;
		this.global_table = global_table;
		if (!SqlUtils.existsTable(c, schema, global_table)) {
			SqlUtils.executeNORES(c, "CREATE TABLE " + schema + "."
					+ global_table + " (" + "full_name text,"
					+ "url text NOT NULL PRIMARY KEY," + "json text" +

					");");
			SqlUtils.execute(c, "SELECT AddGeometryColumn('" + schema + "','"
					+ global_table + "','area','" + Twitter.TWITTER_SRID
					+ "','POLYGON',2);");
		}
		global_insert_stat = c.prepareStatement("INSERT INTO " + schema + "."
				+ global_table + "  (full_name, url, json, area) " + "VALUES"
				+ "  (?, ?, ?, ST_Polygon(ST_GeomFromText(?),"
				+ Twitter.TWITTER_SRID + "));");
		global_insert_null_stat = c.prepareStatement("INSERT INTO " + schema
				+ "." + global_table + "  (full_name, url, json, area) "
				+ "VALUES" + "  (?, ?, ?, ?);");
		global_select_stat = c.prepareStatement("SELECT json FROM " + schema
				+ "." + global_table + " WHERE url=?;");

	}
	
	public static void repairGlobalPlace(Connection c, String schema,
			String table) {
		try {
			PreparedStatement json_update = c.prepareStatement("UPDATE " + schema
					+ "." + table + " SET json=? WHERE url=?;");
			PreparedStatement area_update = c.prepareStatement("UPDATE " + schema
					+ "." + table + " SET area=ST_Polygon(ST_GeomFromText(?),"
				+ Twitter.TWITTER_SRID + ") WHERE url=?;");
			// ResultSet rs = SqlUtils.execute(c, "SELECT url FROM " + schema
			// 		+ "." + table + " WHERE json IS NULL;");
			ResultSet rs = SqlUtils.execute(c, "SELECT url,json FROM " + schema
			 		+ "." + table + " WHERE area IS NULL;");
			while (rs.next()) {
				String url = rs.getString(1);
				String json = rs.getString(2); // getJSON(url);
				String area = null;
				if (json != null) {
					area = getGeometry(json);
				}
				if (json != null) {
					json_update.setString(1, json);
					json_update.setString(2, url);
					json_update.executeUpdate();
					System.out.println("#!Updating json: "+json_update);
				}
				if (area != null) {
					area_update.setString(1, area);
					area_update.setString(2, url);
					area_update.executeUpdate();
					System.out.println("#!Updating area: "+area_update);
				}
			}
			json_update.close();
			area_update.close();
		} catch (SQLException e) {
			System.out.println("#!CAUGHT: " + e);
		}
	}
	
	private static String getJSON(String url) {
		String res = null;
		
		try {
			if ( !blockedByTwitter ) {
				res = WebUtils.getContent(url);
			}
		} catch (Exception e) {
			// twitter refuses accces
			System.out.println("Place:getJSON: Twitter refuses: "+e);
			blockedByTwitter = true;
			// System.exit(0);
		}
		return res;
	}
	
	private static String getGeometry(String json) {
		MyJSONRepository jrep = MyJSONRepository.getRepository(json);
		if (jrep != null) {
			if (jrep.getPath("full_name") == null) {
				// getting bogus from Twitter, invalidate json
				System.out.println("#! twitter URL returns bullshit: "+json);
			} else {
				// System.out.println("Geometry = "+jrep.getPath("geometry", "type"));
				if (jrep.getPath("geometry", "type").toString()
						.equals("Polygon"))
					return jrep.polygon2linestring(jrep.getPath("geometry","coordinates"));
				else {
//					if (jrep.getPath("geometry", "type").toString()
//						.equals("Point"))
//					return jrep.point2linestring(jrep.getPath("geometry","coordinates"));
				}
			}
		}	
		return null;
	}
	
	private String getAreaFromUrl(String url, String full_name) throws SQLException {
		boolean new_tuple = false;
		String area = null;
		
		global_select_stat.setString(1,url);
		ResultSet rs = global_select_stat.executeQuery();
		String json = null;
		if ( rs.next() ) 
			json = rs.getString(1);
		else 
			new_tuple = true;
		if (json == null) 
			json = getJSON(url);
		if ( json != null )
			area = getGeometry(json);
		if (new_tuple) {
			if (area != null) {
				System.out.println("#!resolving place: "+full_name);
				global_insert_stat.setString(1, full_name);
				global_insert_stat.setString(2, url);
				global_insert_stat.setString(3, json);
				global_insert_stat.setString(4, area);
				global_insert_stat.executeUpdate();
			} else {
				global_insert_null_stat.setString(1, full_name);
				global_insert_null_stat.setString(2, url);
				global_insert_null_stat.setString(3, json);
				global_insert_null_stat.setObject(4, area);
				global_insert_null_stat.executeUpdate();

			}
		}
		return area;
	}
	
	private void closeGlobalPlaces() throws SQLException {
		if ( global_insert_stat != null )
			global_insert_stat.close();
			global_insert_null_stat.close();
	}
}
