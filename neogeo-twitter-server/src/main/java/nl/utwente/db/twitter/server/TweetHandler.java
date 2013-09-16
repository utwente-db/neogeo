package nl.utwente.db.twitter.server;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

import nl.utwente.db.named_entity_recog.EntityResolver;
import nl.utwente.db.named_entity_recog.GeoEntity;
import nl.utwente.db.named_entity_recog.NamedEntity;
import nl.utwente.db.named_entity_recog.ResolvedEntity;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class TweetHandler {

	public static void main(String[] args) throws Exception {
		System.out.println("Tweethandler started");
	
		try {
		String s = enrichTweet(EntityResolver.geonames_conn, new Tweet(exampleTweet) );
		System.out.println(s);
		} catch (SQLException e) {
			System.out.println("CAUGHT: "+e);
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static String enrichTweet(Connection c, Tweet t) throws SQLException {
		Vector<NamedEntity> eList = EntityResolver.resolveEntity(t.text(),
				t.lang());

		JSONObject enriched = new JSONObject();
		enriched.put("id", t.id_str());
		enriched.put("tweet", t.text());
		enriched.put("language", t.lang());
		JSONArray geo_list = new JSONArray();
		enriched.put("geolocations", geo_list);
		//

		for (int i = 0; i < eList.size(); i++) {
			NamedEntity ne = eList.get(i);

			Map json_entity = json_named_entity(ne);
			Vector<ResolvedEntity> reList = ne.getResolved();
			for (int j = 0; j < reList.size(); j++) {
				ResolvedEntity re = reList.get(j);

				if (re instanceof GeoEntity) {
					// System.out.println("***SEND: " + re);
					json_add_geolocation(json_entity, (GeoEntity) re);
				} else {
					throw new RuntimeException("UNKOWN ENTITY: " + re);
				}
			}
			geo_list.add(json_entity);
		}

		String enriched_json = enriched.toJSONString();
		enriched_json = convert2uni(enriched_json); // remove unicode char
		
		return enriched_json;
	}
	
	private static final String obj2string(Object o) {
		if ( o != null )
			return o.toString();
		else
			return null;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static Map json_named_entity(NamedEntity ne) {
		Map map = new LinkedHashMap();
		map.put("name", ne.getName());
		map.put("pos", new Integer(ne.getOffset()));
		map.put("candidates",new JSONArray());
		return map;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void json_add_geolocation(Map json_entity, GeoEntity g) {
		JSONArray list = (JSONArray)json_entity.get("candidates");
		
		Map geo_json = new LinkedHashMap();
		geo_json.put("latitude", new Double(g.getLatitude()));
		geo_json.put("longitude", new Double(g.getLongitude()));
		geo_json.put("longitude", new Double(g.getLongitude()));
		geo_json.put("country", g.getCountry());
		geo_json.put("alternatenames", g.getAlternatenames());
		geo_json.put("population", new Integer(g.getPopulation()));
		geo_json.put("elevation", new Integer(g.getElevation()));
		geo_json.put("fclass", g.getFeatureClass());
		geo_json.put("possiblility", new Double(0.5));
		JSONArray links = new JSONArray();
		// links.add(link);
		geo_json.put("links",links);
		list.add(geo_json);
	}
	
	public static String convert2uni(String str) {
		StringBuffer ostr = new StringBuffer();
		for (int i = 0; i < str.length(); i++) {
			char ch = str.charAt(i);
			if ((ch >= 0x0020) && (ch <= 0x007e)) // Does the char need to be
													// converted to unicode?
			{
				ostr.append(ch); // No.
			} else // Yes.
			{
				ostr.append("\\u"); // standard unicode format.
				String hex = Integer.toHexString(str.charAt(i) & 0xFFFF); // Get
																			// hex
																			// value
																			// of
																			// the
																			// char.
				for (int j = 0; j < 4 - hex.length(); j++)
					// Prepend zeros because unicode requires 4 digits
					ostr.append("0");
				ostr.append(hex.toLowerCase()); // standard unicode format.
				// ostr.append(hex.toLowerCase(Locale.ENGLISH));
			}
		}
		return (new String(ostr)); // Return the stringbuffer cast as a string.
	}

	public static String exampleTweet = "{\"truncated\":false,\"text\":\"Ik ging van Enschede naar Almelo dwars door Hengelo onderwijl denkend aan Nepal\",\"in_reply_to_user_id_str\":null,\"geo\":null,\"entities\":{\"hashtags\":[{\"text\":\"unamused\",\"indices\":[87,96]}],\"user_mentions\":[],\"urls\":[]},\"contributors\":null,\"place\":{\"url\":\"http://api.twitter.com/1/geo/id/53afecc4e1db9a21.json\",\"country_code\":\"GB\",\"country\":\"United Kingdom\",\"attributes\":{},\"full_name\":\"Havering, London\",\"name\":\"Havering\",\"id\":\"53afecc4e1db9a21\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[0.137939,51.484156],[0.334433,51.484156],[0.334433,51.635922],[0.137939,51.635922]]]},\"place_type\":\"city\"},\"coordinates\":null,\"source\":\"web\",\"favorited\":false,\"id_str\":\"123780554138193920\",\"retweet_count\":0,\"in_reply_to_screen_name\":null,\"in_reply_to_user_id\":null,\"created_at\":\"Tue Oct 11 15:22:38 +0000 2011\",\"user\":{\"listed_count\":2,\"geo_enabled\":true,\"friends_count\":249,\"profile_sidebar_border_color\":\"EEEEEE\",\"url\":null,\"profile_image_url\":\"http://a2.twimg.com/profile_images/1575195668/image_normal.jpg\",\"lang\":\"nl\",\"profile_use_background_image\":true,\"favourites_count\":3,\"profile_text_color\":\"333333\",\"description\":\"red hair, blue eyes - witty banter and daily sarcasm from 9 till 5.\",\"location\":\"Nashville \",\"default_profile_image\":false,\"statuses_count\":4478,\"profile_background_image_url\":\"http://a1.twimg.com/profile_background_images/325298603/stripes.jpg\",\"default_profile\":false,\"following\":null,\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/325298603/stripes.jpg\",\"profile_link_color\":\"038543\",\"followers_count\":279,\"verified\":false,\"notifications\":null,\"screen_name\":\"SBRAWN\",\"id_str\":\"220478140\",\"show_all_inline_media\":true,\"follow_request_sent\":null,\"contributors_enabled\":false,\"profile_background_color\":\"ACDED6\",\"protected\":false,\"profile_background_tile\":true,\"created_at\":\"Sat Nov 27 22:34:38 +0000 2010\",\"name\":\"shannonjbrawn\",\"time_zone\":null,\"profile_sidebar_fill_color\":\"F6F6F6\",\"id\":220478140,\"is_translator\":false,\"utc_offset\":null,\"profile_image_url_https\":\"https://si0.twimg.com/profile_images/1575195668/image_normal.jpg\"},\"retweeted\":false,\"in_reply_to_status_id\":null,\"id\":123780554138193920,\"in_reply_to_status_id_str\":null}";

}
