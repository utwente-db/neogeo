package nl.utwente.db.twitter.server;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

public class Tweet {

	private String id_str;
	private String tweet;
	@SuppressWarnings("unused")
	private String place;
	@SuppressWarnings("unused")
	private String json;
	private MyJSONRepository jrep;
	
	public Tweet(String json) throws ParseException {
		this.jrep = MyJSONRepository.getRepository(json);
		System.out.println("#!TWEET="+json);
		this.id_str = obj2string(jrep.getPath("id"));
		this.tweet = obj2string(jrep.getPath("text"));
		this.place = place_full_name();
		this.json = json;

	}
	
	public String getJson() {
		return json;
	}

	private static final String obj2string(Object o) {
		if ( o != null )
			return o.toString();
		else
			return null;
	}
	
	public String id_str() {
		// return obj2string( jrep.getPath("id"));
		return this.id_str;
	}
	
	public Long id() {
			return (Long)jrep.getPath("id");
	}
	
	public String text() {
		return obj2string(jrep.getPath("text"));
	}
	
	public String lang() {
		return obj2string(jrep.getPath("lang"));
	}
	
	public String place() {
		// return obj2string( jrep.getPath("place","name")); is different
		// System.out.println("Place["+this.place+"]="+jrep.getPath("place"));
		return obj2string( jrep.getPath("place"));
		// return this.place;
	}
	
	public String place_country_code() {
		return obj2string( jrep.getPath("place","country_code"));
	}
	
	public String place_country() {
		return obj2string( jrep.getPath("place","country"));
	}
	
	public String place_name() {
		return obj2string( jrep.getPath("place","name"));
	}

	public String place_full_name() {
		return obj2string( jrep.getPath("place","full_name"));
	}

	public String place_street_address() {
		return obj2string( jrep.getPath("place","attributes","street_address"));
	}
	
	public String place_type() {
		return obj2string( jrep.getPath("place","place_type"));
	}
	
	public String place_id() {
		return obj2string( jrep.getPath("place","id"));
	}
	
	public String place_url() {
		return obj2string( jrep.getPath("place","url"));
	}
	
	public String tweet() {
		// return obj2string( jrep.getPath("text") );
		return this.tweet;
	}

	public String country() {
		return obj2string( jrep.getPath("place", "country") );
	}

	public String coordinates() {
		return obj2string( jrep.getPath("coordinates") );
	}

	public String coordinatesType() {
		return obj2string( jrep.getPath("coordinates", "type") );
	}
	
	private static final String TWITTER="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
	private static final SimpleDateFormat sf = new SimpleDateFormat(TWITTER);
	
	public Date created_at() {
		String strDate = (String)jrep.getPath("created_at");
		sf.setLenient(true);
		Date res = null;
		try {
			res = sf.parse(strDate);
		} catch(java.text.ParseException e) {
			System.out.println("CAUGHT:Tweet:created_at(): "+e);
		}
		return res;
	}
	
	public String user_screen_name() {
		return obj2string( jrep.getPath("user","screen_name") );
	}

	public String retweeted() {
		return obj2string( jrep.getPath("retweeted") );
	}
	
	public String in_reply_to_status_id() {
		return obj2string( jrep.getPath("in_reply_to_status_id_str") );
	}

	public String coordinatesValue() {
		Object lo =  jrep.getPath("coordinates", "coordinates");
		
		if ( lo != null && lo instanceof List ) {
			@SuppressWarnings("rawtypes")
			List l = (List)lo;
			StringBuffer res = new StringBuffer();
			res.append("POINT(");
			res.append(MyJSONRepository.obj2double(l.get(0)));
			res.append(" ");
			res.append(MyJSONRepository.obj2double(l.get(1)));
			res.append(")");
			return res.toString();

		} else
			return null;
	}
	
	public String place_bbox() {
		Object lo = jrep.getPath("place","bounding_box","coordinates");
		
		if ( lo != null && lo instanceof LinkedList ) {
			@SuppressWarnings("rawtypes")
			List l = (List)((LinkedList)lo).get(0);
			
			if ( l.size() == 4) {
				// so it must be a proper bounding box
				StringBuffer res  = new StringBuffer();
				res.append("LINESTRING(");
				
				double dc1, dc2, df1, df2;
				dc1 = dc2= df1 = df2 = -1;
				for(int i=0; i<4; i++) {
					@SuppressWarnings("rawtypes")
					List pi = (List)l.get(i);
					
					dc1 = MyJSONRepository.obj2double(pi.get(0));
					dc2 = MyJSONRepository.obj2double(pi.get(1));
					
					if ( i == 0 ) {
						df1 = dc1;
						df2 = dc2;
					} else 
						res.append(",");
					res.append(dc1);
					res.append(" ");
					res.append(dc2);
					if ( i == 3 ) {
						if ( !(dc1==df1 && dc2==df2) ) {
							// close the polygon
							res.append(",");
							res.append(df1);
							res.append(" ");
							res.append(df2);
						}
							
					}
				}
				res.append(")");
				// System.out.println("RAW="+jrep.getPath("place"));
				// System.out.println("PLACE="+res);
				return res.toString();
			}
		}
		throw new RuntimeException("UNEXPECTED EMPTY BBOX FOR PLACE");
		// return null;
	}
	
	@SuppressWarnings("unchecked")
	public String dummyEnriched() {
		JSONObject obj=new JSONObject();
		  obj.put("id",this.id());
		  obj.put("tweet",this.tweet());
		  obj.put("language","English");
		  //
		  JSONArray list = new JSONArray();
		  list.add(json_geolocation("Nowhereistan",0,"(x,y)",0.98,"htpp:/url/nws"));
		  list.add(json_geolocation("Somewhereistan",37,"(x,y)",0.95,"htpp:/url/sws"));
		  list.add(json_geolocation("Anywhereistan",92,"(x,y)",1.0,"htpp:/url/aws"));
		  obj.put("geolocations", list);
		  //
		  String rawres = obj.toJSONString();
		  rawres = convert2uni( rawres ); // remove unicode chars
		  //
		  return rawres;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Map json_geolocation(String name, int pos, String geolocation, double possibility, String link) {
		Map map = new LinkedHashMap();
		map.put("name", name);
		map.put("pos", new Integer(pos));
		JSONArray list = new JSONArray();
		Map map2 = new LinkedHashMap();
		map2.put("geolocation", geolocation);
		map2.put("possiblility", new Double(possibility));
		JSONArray links = new JSONArray();
		links.add(link);
		map2.put("links",link);
		list.add(map2);
		map.put("candidates",list);
		return map;
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

	public static String exampleTweet = "{\"truncated\":false,\"text\":\"Need \\u25ba to sort out the home broadband, its becoming far too temperamental for my liking! #unamused\",\"in_reply_to_user_id_str\":null,\"geo\":null,\"entities\":{\"hashtags\":[{\"text\":\"unamused\",\"indices\":[87,96]}],\"user_mentions\":[],\"urls\":[]},\"contributors\":null,\"place\":{\"url\":\"http://api.twitter.com/1/geo/id/53afecc4e1db9a21.json\",\"country_code\":\"GB\",\"country\":\"United Kingdom\",\"attributes\":{},\"full_name\":\"Havering, London\",\"name\":\"Havering\",\"id\":\"53afecc4e1db9a21\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[0.137939,51.484156],[0.334433,51.484156],[0.334433,51.635922],[0.137939,51.635922]]]},\"place_type\":\"city\"},\"coordinates\":null,\"source\":\"web\",\"favorited\":false,\"id_str\":\"124780554138193920\",\"retweet_count\":0,\"in_reply_to_screen_name\":null,\"in_reply_to_user_id\":null,\"created_at\":\"Tue Oct 11 15:22:38 +0000 2011\",\"user\":{\"listed_count\":2,\"geo_enabled\":true,\"friends_count\":249,\"profile_sidebar_border_color\":\"EEEEEE\",\"url\":null,\"profile_image_url\":\"http://a2.twimg.com/profile_images/1575195668/image_normal.jpg\",\"lang\":\"en\",\"profile_use_background_image\":true,\"favourites_count\":3,\"profile_text_color\":\"333333\",\"description\":\"red hair, blue eyes - witty banter and daily sarcasm from 9 till 5.\",\"location\":\"Nashville \",\"default_profile_image\":false,\"statuses_count\":4478,\"profile_background_image_url\":\"http://a1.twimg.com/profile_background_images/325298603/stripes.jpg\",\"default_profile\":false,\"following\":null,\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/325298603/stripes.jpg\",\"profile_link_color\":\"038543\",\"followers_count\":279,\"verified\":false,\"notifications\":null,\"screen_name\":\"SBRAWN\",\"id_str\":\"220478140\",\"show_all_inline_media\":true,\"follow_request_sent\":null,\"contributors_enabled\":false,\"profile_background_color\":\"ACDED6\",\"protected\":false,\"profile_background_tile\":true,\"created_at\":\"Sat Nov 27 22:34:38 +0000 2010\",\"name\":\"shannonjbrawn\",\"time_zone\":null,\"profile_sidebar_fill_color\":\"F6F6F6\",\"id\":22478140,\"is_translator\":false,\"utc_offset\":null,\"profile_image_url_https\":\"https://si0.twimg.com/profile_images/1575195668/image_normal.jpg\"},\"retweeted\":false,\"in_reply_to_status_id\":null,\"id\":123780554138193920,\"in_reply_to_status_id_str\":null}";

	public static String exampleTweet2 = "{\"truncated\":false,\"text\":\"Need \\u25ba to sort out the home broadband, its becoming far too temperamental for my liking! #unamused\",\"in_reply_to_user_id_str\":null,\"geo\":null,\"entities\":{\"hashtags\":[{\"text\":\"unamused\",\"indices\":[87,96]}],\"user_mentions\":[],\"urls\":[]},\"contributors\":null,\"place\":{\"url\":\"http://api.twitter.com/1/geo/id/53afecc4e1db9a21.json\",\"country_code\":\"GB\",\"country\":\"United Kingdom\",\"attributes\":{},\"full_name\":\"Havering, London\",\"name\":\"Havering\",\"id\":\"53afecc4e1db9a21\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[0.137939,51.484156],[0.334433,51.484156],[0.334433,51.635922],[0.137939,51.635922]]]},\"place_type\":\"city\"},\"coordinates\":null,\"source\":\"web\",\"favorited\":false,\"id_str\":\"124780554138193920\",\"retweet_count\":0,\"in_reply_to_screen_name\":null,\"in_reply_to_user_id\":null,\"created_at\":\"Tue Oct 11 15:22:38 +0000 2011\",\"user\":{\"listed_count\":2,\"geo_enabled\":true,\"friends_count\":249,\"profile_sidebar_border_color\":\"EEEEEE\",\"url\":null,\"profile_image_url\":\"http://a2.twimg.com/profile_images/1575195668/image_normal.jpg\",\"lang\":\"en\",\"profile_use_background_image\":true,\"favourites_count\":3,\"profile_text_color\":\"333333\",\"description\":\"red hair, blue eyes - witty banter and daily sarcasm from 9 till 5.\",\"location\":\"Nashville \",\"default_profile_image\":false,\"statuses_count\":4478,\"profile_background_image_url\":\"http://a1.twimg.com/profile_background_images/325298603/stripes.jpg\",\"default_profile\":false,\"following\":null,\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/325298603/stripes.jpg\",\"profile_link_color\":\"038543\",\"followers_count\":279,\"verified\":false,\"notifications\":null,\"screen_name\":\"SBRAWN\",\"id_str\":\"220478140\",\"show_all_inline_media\":true,\"follow_request_sent\":null,\"contributors_enabled\":false,\"profile_background_color\":\"ACDED6\",\"protected\":false,\"profile_background_tile\":true,\"created_at\":\"Sat Nov 27 22:34:38 +0000 2010\",\"name\":\"shannonjbrawn\",\"time_zone\":null,\"profile_sidebar_fill_color\":\"F6F6F6\",\"id\":22478140,\"is_translator\":false,\"utc_offset\":null,\"profile_image_url_https\":\"https://si0.twimg.com/profile_images/1575195668/image_normal.jpg\"},\"retweeted\":false,\"in_reply_to_status_id\":null,\"id\":123780555138193920,\"in_reply_to_status_id_str\":null}";

}