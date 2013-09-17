package nl.utwente.db.neogeo.twitter.harvest;

import java.util.Vector;

import nl.utwente.db.neogeo.twitter.harvest.type.Place;
import nl.utwente.db.neogeo.twitter.harvest.type.SampleTweet;
import nl.utwente.db.neogeo.twitter.harvest.type.filterTweet;

import org.basex.io.serial.JSONSerializer;
import org.json.simple.JSONObject;


public class JSONParser {

	public static JSONObject getTOPJSONObject(String jsonTxt) throws Exception {
		JSONObject json = (JSONObject) JSONSerializer.toJSON(jsonTxt);
		return json;
	}

	public static filterTweet parseFilterTweet(String jsonTxt) throws Exception {
		JSONObject json = getTOPJSONObject(jsonTxt);
		String idstr = json.getString("id_str");
		String tweet = json.getString("text");
		String time = json.getString("created_at");
		JSONObject objPlace = json.getJSONObject("place");
		String place = null;
		if (objPlace != null)
			place = objPlace.getString("full_name");
		return new filterTweet(json.getString("id_str"),
				json.getString("text"), json.getString("created_at"), place,
				jsonTxt);
	}
	
	public static String getTweetText(String jsonTxt){
		try{
			JSONObject json = getTOPJSONObject(jsonTxt);
			return json.getString("text");
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
	}
	

	public static SampleTweet parseSampleTweet(String jsonTxt) throws Exception {
		JSONObject json = getTOPJSONObject(jsonTxt);
		System.out.println(jsonTxt);
		String place = json.getString("place");
		if (place.equals("null")) {
			place = json.getJSONObject("user").getString("location");
		}
		return new SampleTweet(json.getString("id_str"),
				json.getString("text"), json.getString("created_at"), place,
				jsonTxt);
	}

	public static String getIDStr(String jsonTxt) throws Exception {
		JSONObject json = getTOPJSONObject(jsonTxt);
		return json.getString("id_str");
	}

	/**
	 * parse the json text as shown in:
	 * https://dev.twitter.com/docs/api/1/get/geo/reverse_geocode
	 * 
	 * the result will be a vector of Place objects
	 */
	public static Vector<Place> parseReverseGeoCode(String jsonTxt) {
		Vector<Place> result = new Vector<Place>();
		try {
			JSONObject topObj = getTOPJSONObject(jsonTxt);
			JSONArray places = topObj.getJSONObject("result").getJSONArray(
					"places");
			for (Object obj : places) {
				JSONObject place = (JSONObject) obj;
				String name = place.getString("name");
				String country = place.getString("country");
				String countryCode = place.getString("country_code");
				String id = place.getString("id");
				Place p = new Place();
				p.m_name = name;
				p.m_country = country;
				p.m_countryCode = countryCode;
				p.m_id = id;
				result.add(p);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

}
