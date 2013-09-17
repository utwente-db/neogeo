package nl.utwente.db.neogeo.twitter.harvest;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Vector;

import nl.utwente.db.neogeo.twitter.harvest.type.Place;

/**
 * @author ZhuZ
 * @date 29-09-2011 There are three categories of APIs provided by Twitter: More
 *       details can be found here: https://dev.twitter.com/docs/api
 */
public class TwitterAPIWrapper {

	/**
	 * Implement Twitter Stream API: statuses/firehose
	 * https://dev.twitter.com/docs/streaming-api/methods
	 * 
	 * Note: access to this method requires special arrangement with Twitter. If
	 * the Http response code is 403, then means the user has forbidden to
	 * access this resource. See more details in the "Access and Rate Limiting"
	 * section on:
	 * https://dev.twitter.com/docs/streaming-api/concepts#access-rate-limiting
	 * 
	 * 
	 */
	public static void streamStatusesFirehose(String user, String password) {
		InputStream input = HTTPRequest.streamByGET(
				"https://stream.twitter.com/1/statuses/firehose.json",
				"zheminzhu", "pipp3omi", "count=10");
		BufferedReader rd = new BufferedReader(new InputStreamReader(input));
		String line;
		try {
			while ((line = rd.readLine()) != null) {
				System.out.print(line);
			}
			input.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * GET geo/reverse_geocode
	 * https://dev.twitter.com/docs/api/1/get/geo/reverse_geocode
	 * 
	 * Given a latitude and a longitude, searches for up to 20 places that can
	 * be used as a place_id when updating a status. This request is an
	 * informative call and will deliver generalized results about geography.
	 */
	public static Vector<Place> getPlaces(double latitude, double longitude,
			String... optionals) {

		final String endpoint = "http://api.twitter.com/1/geo/reverse_geocode.json";
		Vector<String> params = new Vector<String>();
		params.add("lat=" + latitude);
		params.add("long=" + longitude);
		for (String optional : optionals)
			params.add(optional);
		String getRequest = HTTPRequest.formatRequest(endpoint, params);
		String serverReturn = HTTPRequest.sendGetRequest(getRequest);
		return JSONParser.parseReverseGeoCode(serverReturn);
	}

}
