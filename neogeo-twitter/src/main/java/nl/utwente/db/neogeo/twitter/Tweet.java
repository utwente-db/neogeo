package nl.utwente.db.neogeo.twitter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class Tweet {

	private String id_str;
	private String tweet;
	@SuppressWarnings("unused")
	private String place;
	@SuppressWarnings("unused")
	private String json;
	private MyJSONRepository jrep;
	private String	errorMessage = null;

	public Tweet(String id_str, String tweet, String place, String json) {
		this.id_str = id_str;
		this.tweet = tweet;
		this.place = place;
		this.json = json;
		this.jrep = MyJSONRepository.getRepository(json);
	}
	
	public Tweet(String json) {
		this.jrep = MyJSONRepository.getRepository(json);
		this.id_str = obj2string( jrep.getPath("id"));
		this.tweet = obj2string( jrep.getPath("text"));
		this.place = place_full_name();
		this.json = json;
	}

	public boolean isValid() {
		return errorMessage == null;
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
		} catch(ParseException e) {
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
	
	public static String exampleTweet = "{\"truncated\":false,\"text\":\"Need to sort out the home broadband, its becoming far too temperamental for my liking! #unamused\",\"in_reply_to_user_id_str\":null,\"geo\":null,\"entities\":{\"hashtags\":[{\"text\":\"unamused\",\"indices\":[87,96]}],\"user_mentions\":[],\"urls\":[]},\"contributors\":null,\"place\":{\"url\":\"http://api.twitter.com/1/geo/id/53afecc4e1db9a21.json\",\"country_code\":\"GB\",\"country\":\"United Kingdom\",\"attributes\":{},\"full_name\":\"Havering, London\",\"name\":\"Havering\",\"id\":\"53afecc4e1db9a21\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[0.137939,51.484156],[0.334433,51.484156],[0.334433,51.635922],[0.137939,51.635922]]]},\"place_type\":\"city\"},\"coordinates\":null,\"source\":\"web\",\"favorited\":false,\"id_str\":\"123780554138193920\",\"retweet_count\":0,\"in_reply_to_screen_name\":null,\"in_reply_to_user_id\":null,\"created_at\":\"Tue Oct 11 15:22:38 +0000 2011\",\"user\":{\"listed_count\":2,\"geo_enabled\":true,\"friends_count\":249,\"profile_sidebar_border_color\":\"EEEEEE\",\"url\":null,\"profile_image_url\":\"http://a2.twimg.com/profile_images/1575195668/image_normal.jpg\",\"lang\":\"en\",\"profile_use_background_image\":true,\"favourites_count\":3,\"profile_text_color\":\"333333\",\"description\":\"red hair, blue eyes - witty banter and daily sarcasm from 9 till 5.\",\"location\":\"Nashville \",\"default_profile_image\":false,\"statuses_count\":4478,\"profile_background_image_url\":\"http://a1.twimg.com/profile_background_images/325298603/stripes.jpg\",\"default_profile\":false,\"following\":null,\"profile_background_image_url_https\":\"https://si0.twimg.com/profile_background_images/325298603/stripes.jpg\",\"profile_link_color\":\"038543\",\"followers_count\":279,\"verified\":false,\"notifications\":null,\"screen_name\":\"SBRAWN\",\"id_str\":\"220478140\",\"show_all_inline_media\":true,\"follow_request_sent\":null,\"contributors_enabled\":false,\"profile_background_color\":\"ACDED6\",\"protected\":false,\"profile_background_tile\":true,\"created_at\":\"Sat Nov 27 22:34:38 +0000 2010\",\"name\":\"shannonjbrawn\",\"time_zone\":null,\"profile_sidebar_fill_color\":\"F6F6F6\",\"id\":220478140,\"is_translator\":false,\"utc_offset\":null,\"profile_image_url_https\":\"https://si0.twimg.com/profile_images/1575195668/image_normal.jpg\"},\"retweeted\":false,\"in_reply_to_status_id\":null,\"id\":123780554138193920,\"in_reply_to_status_id_str\":null}";

}