package nl.utwente.db.named_entity_recog;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;

import nl.utwente.db.neogeo.utils.WebUtils;

public class TomTom {
	
	private GeoNameEntityTable entityDB  = null;
	
	public TomTom(Connection c) throws SQLException {
		this.entityDB = new GeoNameEntityTable( GeoNamesDB.geoNameDBConnection() );
	}
	
	public  void test() throws IOException, SQLException {
		readRestJson("/Users/flokstra/tomtom/Restaurants50/vrt-rest-0000_0050.json");
	}
	
	public  void readRestJson(String fileName) throws IOException, SQLException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		JsonReader jsonReader = Json.createReader(br);
		JsonObject jo = jsonReader.readObject(); // incomplete, check errors
        jsonReader.close();
        //
        JsonObject response = jo.getJsonObject("response");
        JsonObject data = response.getJsonObject("data");
        System.out.println("data="+data);
        JsonObject data_main = data.getJsonObject("main");
        System.out.println("Total #requests="+data_main.getInt("totalNumberOfResults"));
        JsonArray places = data.getJsonObject("places").getJsonArray("place");
        for (int i=0; i<places.size(); i++) {
        	// incomplete: [name,street,city,   ,category,category_id,lat,lon]
        	JsonObject place = places.getJsonObject(i);
        	System.out.println("place: "+place);
        	//
        	JsonObject name = place.getJsonObject("name");
        	String name_str = name.getString("$");
        	System.out.println("Name: "+name_str);
        	//
        	int first_category_nr = 0;
        	String category_str = null;
        	
        	if ( true ) {
        		JsonArray categories = place.getJsonObject("categories").getJsonArray("category");
                for (int j=0; j<categories.size(); j++) {
                	JsonObject category = categories.getJsonObject(j);
                	String newcat = category.getString("name");
                	int newcat_id = new Integer(category.getString("@id")).intValue();
                	System.out.println("Categorie: "+newcat);
                	if ( first_category_nr == 0 ) {
                		first_category_nr = newcat_id;
                		category_str = newcat;
                	} else {
                		category_str += (","+newcat);
                	}
                	entityDB.defineCategory(newcat_id, newcat);
                	System.out.println("Cat#: "+first_category_nr);
                }
                // INCOMPLETE: als laatste, voor elke category 1 record genereren.
                // INCOMPLETE: id klopt niet met lijst op website, ????, make auto tabel, alles lc
                // tabel: tt_poi_category [name,id]

        	}
        	//
        	JsonObject city = place.getJsonObject("city");
        	String city_str = city.getString("$");
        	System.out.println("City: "+city_str);
        	// INCOMPLETE: province
        	// INCOMPLETE: adres/postcode
        	//
        	String fmaddr_str = place.getString("formattedAddress");
        	// String fmaddr_str = city.getString("$");
        	System.out.println("Full Adres: "+fmaddr_str);
        	//
        	JsonNumber lat = place.getJsonNumber("latitude");
        	JsonNumber lon = place.getJsonNumber("longitude");
        	System.out.println("lat/lon="+lat+"/"+lon);
        	//
        	if ( true ) {
        		entityDB.insertEntity(name_str, new Integer(first_category_nr).intValue(), category_str, city_str, "Overijssel", fmaddr_str, 1/*count_nl*/, lon.doubleValue(), lat.doubleValue());
        	}
        }
		
	}
	
	private static int step = 50;
	private static int limit = 1750; // 1750 rest / 58350 voor all	
	public static final String restaurantDir = "/Users/flokstra/tomtom/Restaurants_RAW/";
	public static final String allplaceDir = "/Users/flokstra/tomtom/VrtAll_RAW/";
	public static final String baseDir = restaurantDir;

	
	public  String restFileName(String base, String pfx, String kind, int from, int step) {
		return base+pfx+"-"+kind+"-"+(from)+"_"+(from+step)+".json"; 
	}
	
	public  void test2() throws IOException {	
		String what = "&what=Restaurants";
		int count = 0;
		while ( count < limit ) {
			String url="http://api.tomtom.com/places/search/1/place?key=aadnffgkrxfkbrhbp4mgvguz&sw=52.0,6.2&ne=52.5,7.2"+what+"&results="+step+"&start="+count+"&format=json&searchId=249cfb70-f2ef-11e3-8ce6-ea75e91104fd";
			System.out.println("- "+count+"-"+(count+step));
			System.out.println(url);
			url2file(url, restFileName(baseDir,"vrt","rest",count,step));
			count += step;
		}
	}
	
	public  void test2all() throws IOException {	
		String what = "";
		int count = 0;
		while ( count < 58350 ) {
			String url="http://api.tomtom.com/places/search/1/place?key=aadnffgkrxfkbrhbp4mgvguz&sw=52.0,6.2&ne=52.5,7.2"+what+"&results="+step+"&start="+count+"&format=json&searchId=249cfb70-f2ef-11e3-8ce6-ea75e91104fd";
			System.out.println("- "+count+"-"+(count+step));
			System.out.println(url);
			url2file(url, restFileName(allplaceDir,"vrt","all",count,step));
			count += step;
		}
	}
	
	public  void test3() throws IOException, SQLException  {	
		int count = 0;
		while ( count < limit ) {
			readRestJson(restFileName(baseDir,"vrt","rest",count,step));
			count += step;
		}
	}
	
	public  void test4() throws IOException, SQLException {	
		int count = 0;
		
		while ( count < 58350 ) {
		// while ( count < 50 ) {
			readRestJson(restFileName(allplaceDir,"vrt","all",count,step));
			count += step;
		}
	}
	public static void url2file(String url, String fileName) throws IOException {
		String doc = WebUtils.getContent(url);
		if ( doc == null )
			throw new FileNotFoundException("URL not found: "+url);
		
		PrintWriter out = new PrintWriter(fileName);
		
		out.print(doc);
		out.close();
	}
	
	public static void main(String[] args) {
		try {
			TomTom tomtom = new TomTom( GeoNamesDB.geoNameDBConnection() );
			tomtom.test4();
		}	catch (Exception e) {
			System.out.println("CAUGHT: "+e);
			e.printStackTrace();
		}
    }
}