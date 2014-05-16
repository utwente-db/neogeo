package nl.utwente.db.named_entity_recog;

import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonGeneratorFactory;

public class RequestHandler {
	
	public RequestHandler() {
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
		
		JsonArray entity_kind = area.getJsonArray("entity_kind");
		NationaalRegisterStreetDB nrgStreetDB = new NationaalRegisterStreetDB( GeoNamesDB.geoNameDBConnection() );
		ResultSet rs = null;
		
		if ( kind.equals("bbox") ) {
			JsonObject bbox = area.getJsonObject("bbox");
			double sw_lon = new Double(bbox.getString("sw_lon")).doubleValue();
			double sw_lat = new Double(bbox.getString("sw_lat")).doubleValue();
			double ne_lon = new Double(bbox.getString("ne_lon")).doubleValue();
			double ne_lat = new Double(bbox.getString("ne_lat")).doubleValue();
			//
			rs = nrgStreetDB.streetBoxCount(label,0,sw_lon,sw_lat,ne_lon,ne_lat);
		} else
			return _input_error("kind should be bbox: "+kind);
		jg.writeStartObject();
		 jg.write("label", label);
		 jg.write("limit", limit);
		 // jg.write("entity_kind", entity_kind);
		 jg.write("area",area);
		 jg.writeStartObject("result");
		  jg.writeStartObject("street");
		   jg.write("count_nl",271924);
		   JsonGenerator jg2 = jg.writeStartArray("list");
		   while ( rs.next() ) {
		     jg2.writeStartObject();
		     jg2.write("name",rs.getString("name"));
		     jg2.write("city",rs.getString("city"));
		     jg2.write("category",rs.getInt("category"));
		     jg2.write("lon",rs.getDouble("lon"));
		     jg2.write("lat",rs.getDouble("lat"));
		     jg2.write("count_nl",rs.getDouble("count_nl"));
		     jg2.write("count_area",rs.getDouble("count_area"));
		     jg2.writeEnd();
		    }
		   jg.writeEnd();
		  jg.writeEnd();
		 jg.writeEnd();
		jg.writeEnd();
		//
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			RequestHandler rh = new RequestHandler();
			
			String json_in = "{ \"area\": { \"srid\": \"4326\", \"label\": \"enschede_c\", \"cache\": \"create\", \"kind\" : \"bbox\", \"bbox\": { \"sw_lon\": \"6.9\", \"sw_lat\": \"52.2167\", \"ne_lon\": \"6.95\", \"ne_lat\": \"52.22\" } }, \"entity_kind\": [ \"Street\" ], \"result_kind\": \"list\", \"format\": \"json\", \"limit\": 100 }";			
		    System.out.println(json_in);
			BufferedReader br = new BufferedReader(new StringReader(json_in));
			rh.handleGeoEntityFinder(br,new PrintWriter(System.out));
		}	catch (SQLException e) {
			System.out.println("CAUGHT: "+e);
			e.printStackTrace();
		}
    }
}