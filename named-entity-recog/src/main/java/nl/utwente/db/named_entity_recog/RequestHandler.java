package nl.utwente.db.named_entity_recog;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;

import nl.utwente.db.neogeo.twitter.SqlUtils;

public class RequestHandler {
	
	private Connection c;
	GeoNameEntityTable entityDB = null;
	
	public RequestHandler(Connection c) throws SQLException {
		this.c = c;
		this.entityDB = new GeoNameEntityTable( GeoNamesDB.geoNameDBConnection() );
	}
	
	public int handleGeoEntityFinder(BufferedReader br, PrintWriter writer) throws SQLException {
		// create json input reader
		JsonReader jsonReader = Json.createReader(br);
		//get JsonObject from JsonReader
        JsonObject jsonObject = jsonReader.readObject(); // incomplete, check errors
        jsonReader.close();
        
		// create json output generator
		JsonGeneratorFactory factory = Json.createGeneratorFactory(null);
		JsonGenerator generator = factory.createGenerator(writer);
		int result = _handleGeoEntityFinder(jsonObject, generator);
		generator.close();
		//
		return result; // no error
	}
	
	private int _input_error(String message) {
		System.err.println("ERROR: "+message);
		return 500; // http error code
	}
	
	private int _handleGeoEntityFinder(JsonObject jo,  JsonGenerator jg) throws SQLException {
		JsonObject area = jo.getJsonObject("area");
		String kind = area.getString("kind");
		String srid = area.getString("srid");
		String label = area.getString("label");
		int limit = jo.getInt("limit");
		
		JsonArray entity_kind = jo.getJsonArray("entity_kind");
		Vector<Integer> categories = new Vector<Integer>();
		for (int i=0; i<entity_kind.size(); i++) {
			entityDB.handleSelectCategory(entity_kind.getString(i), categories);
		}
		NationaalRegisterStreetDB nrgStreetDB = new NationaalRegisterStreetDB( GeoNamesDB.geoNameDBConnection() );
		ResultSet rs = null;
		
		if ( kind.equals("bbox") ) {
			JsonObject bbox = area.getJsonObject("bbox");
			double sw_lon = new Double(bbox.getString("sw_lon")).doubleValue();
			double sw_lat = new Double(bbox.getString("sw_lat")).doubleValue();
			double ne_lon = new Double(bbox.getString("ne_lon")).doubleValue();
			double ne_lat = new Double(bbox.getString("ne_lat")).doubleValue();
			//
			rs = streetANDpoiBoxCount(label,categories,0,sw_lon,sw_lat,ne_lon,ne_lat);
		} else
			return _input_error("kind should be bbox: "+kind);
		jg.writeStartObject();
		 jg.write("label", label);
		 jg.write("limit", limit);
		 // jg.write("entity_kind", entity_kind);
		 jg.write("area",area);
		 jg.writeStartObject("result");
		  // jg.writeStartObject("street");
		   jg.write("count_nl",271924);
		   JsonGenerator jg2 = jg.writeStartArray("list");
		   while ( rs.next() ) {
		     jg2.writeStartObject();
		     jg2.write("name",rs.getString("name"));
		     jg2.write("city",rs.getString("city"));
		     int category = rs.getInt("category");
		     jg2.write("category",category);
		     jg2.write("categories",rs.getString("category_str"));
		     if ( category != 1000 ) {
		    	 jg.write("addres",rs.getString("addres"));
		     }
		     jg2.write("lon",rs.getDouble("lon"));
		     jg2.write("lat",rs.getDouble("lat"));
		     jg2.write("count_nl",rs.getDouble("count_nl"));
		     jg2.write("count_area",rs.getDouble("count_area"));
		     jg2.writeEnd();
		    }
		   jg.writeEnd();
		  // jg.writeEnd();
		 jg.writeEnd();
		jg.writeEnd();
		//
		return 0;
	}
	
	//
	//
	//
	
	private final String area_cache_raw = "areac_raw_";
	private final String area_cache_gb = "areac_gb_";
	
	private String bbox_cond(double sw_lat, double sw_lon, double ne_lat, double ne_lon, int srid) {
		return "ST_Contains(" +
					"ST_SetSRID(" +
						"ST_MakeBox2D(" +
							"ST_Point("+sw_lat+","+sw_lon+"), ST_Point("+ne_lat+","+ne_lon+")" +
						"), " +
						srid +
					")" +
				", coordinates" +
				")";
	}
	
	public void handleCacheStart(String label, int option, double sw_lat, double sw_lon, double ne_lat, double ne_lon) throws SQLException {
		String cache_rawname = area_cache_raw+label;
		String cache_gbname = area_cache_gb+label;
		
		if ( SqlUtils.existsTable(c, NationaalRegisterStreetDB.csSchema, cache_rawname))
			SqlUtils.dropTable(c, NationaalRegisterStreetDB.csSchema, cache_rawname);
		if ( SqlUtils.existsTable(c, NationaalRegisterStreetDB.csSchema, cache_gbname))
			SqlUtils.dropTable(c, NationaalRegisterStreetDB.csSchema, cache_gbname);
		String hdr = 
			"SELECT city,street AS name,1000 AS category, \'Streets\' AS category_str, \'\' AS addres, st_x(coordinates) AS lon,st_y(coordinates) AS lat,count_nl INTO " + NationaalRegisterStreetDB.csSchema + "." + cache_rawname+" " +
			"FROM "+ NationaalRegisterStreetDB.csSchema + "." + "nrg_street " +
			"WHERE " + bbox_cond(sw_lat,sw_lon,ne_lat,ne_lon,4326);
		if ( true ) {
			String category_cond = "";
			
			// category_cond = " AND category=8481 ";
			hdr = hdr + " UNION (" +
				"SELECT city,name,category,category_str,extra as addres, st_x(coordinates) AS lon,st_y(coordinates) AS lat,count_nl " +
				"FROM "+ NationaalRegisterStreetDB.csSchema + "." + "geoentity " +
				"WHERE " + bbox_cond(sw_lat,sw_lon,ne_lat,ne_lon,4326) + category_cond + ")";
		}
		PreparedStatement ps = null;
		// System.out.println(hdr);
		ps = c.prepareStatement(hdr + ";");
		
		ps.execute();
		
		ps = c.prepareStatement(
				"CREATE INDEX "+cache_rawname+"_nidx" + " ON " +  NationaalRegisterStreetDB.csSchema + "." + cache_rawname + " USING hash(name);"
			);
		ps.execute();
		
		//
		//
		//
		
		ps = c.prepareStatement(
				"SELECT name,count(name) AS count_area INTO " + NationaalRegisterStreetDB.csSchema + "." + cache_gbname +
				" FROM "+NationaalRegisterStreetDB.csSchema + "." + cache_rawname+" GROUP BY name;"
			 );
		ps.execute();
		
		ps = c.prepareStatement(
				"CREATE INDEX "+cache_gbname+"_nidx" + " ON " +  NationaalRegisterStreetDB.csSchema + "." + cache_gbname + " USING hash(name);"
			);
		ps.execute();
	}
	
	public void handleCacheEnd(String label, int option) throws SQLException {
		// SqlUtils.dropTable(c, csSchema, area_cache_raw+label);
		// SqlUtils.dropTable(c, csSchema, area_cache_gb+label);
	}
	
	public ResultSet streetANDpoiBoxCount(String label, Vector<Integer> categories, int option, double sw_lat, double sw_lon, double ne_lat, double ne_lon) throws SQLException {
		long startTime = System.currentTimeMillis();
		handleCacheStart(label,option,sw_lat,sw_lon,ne_lat,ne_lon);
		
		StringBuffer category_select = new StringBuffer("");
		
		if ( (categories != null) && (!entityDB.allSelectCategory(categories)) ) {
			// category_select = "AND (category=8481)";
			category_select.append("AND (");
			for (int i=0; i<categories.size(); i++) {
				if ( i>0 )
					category_select.append(" OR ");
				category_select.append("category="+categories.get(i));
			}
			category_select.append(")");
		}
		PreparedStatement ps = c.prepareStatement(
				"SELECT all_ent.name AS name,city,category,category_str,addres,lon,lat,count_nl,count_area " +
				"FROM " + NationaalRegisterStreetDB.csSchema + "." + (area_cache_raw+label) + " AS all_ent , " + NationaalRegisterStreetDB.csSchema + "." + (area_cache_gb+label) + " AS gb_ent " +
				"WHERE (all_ent.name = gb_ent.name)"+category_select+";"
		    );
		System.out.println("QUERY: "+ps);
		
		handleCacheEnd(label,option);
		long elapsedTime = System.currentTimeMillis() - startTime;
		if ( false ) 
			System.out.println("Elapsed time: "+elapsedTime + "ms.");
		
		return ps.executeQuery();
	}
	
	//
	//
	//
	
	private static final String create_lat_lon_json(double sw_lat, double sw_lon, double ne_lat, double ne_lon) {
		return "\"bbox\": { \"sw_lon\": \""+sw_lon+"\", \"sw_lat\": \""+sw_lat+"\", \"ne_lon\": \""+ne_lon+"\", \"ne_lat\": \""+ne_lat+"\"";
	}
	
	public static String debugRequest() {
		
		String categories = "\"All\"";
		// String categories = "\"Streets\",\"Accountants\",\"8481\"";
		String bbox = create_lat_lon_json(52.2167,6.9,52.22,6.95); // Enschede center
		double lat = 53.2;
		double lon = 6.5;
		double delta = 0.01;
		bbox = create_lat_lon_json((lat-delta),(lon-delta),(lat+delta),(lon+delta)); // Groningen center
		return "{ \"area\": { \"srid\": \"4326\", \"label\": \"enschede_c\", \"cache\": \"create\", \"kind\" : \"bbox\", "+bbox+"} }, \"entity_kind\": [ "+categories+" ], \"result_kind\": \"list\", \"format\": \"json\", \"limit\": 100 }";	
	}
	
	public static void main(String[] args) {
		try {
			RequestHandler rh = new RequestHandler(GeoNamesDB.geoNameDBConnection());
			
			if ( false ) {
				rh.streetANDpoiBoxCount("xxx",null,0,6.9, 52.2167, 6.95, 52.22);
				System.exit(0);
			}
		    System.out.println("REQUEST: "+debugRequest());
			BufferedReader br = new BufferedReader(new StringReader(debugRequest()));
			rh.handleGeoEntityFinder(br,new PrintWriter(System.out));
		}	catch (SQLException e) {
			System.out.println("CAUGHT: "+e);
			e.printStackTrace();
		}
    }
}