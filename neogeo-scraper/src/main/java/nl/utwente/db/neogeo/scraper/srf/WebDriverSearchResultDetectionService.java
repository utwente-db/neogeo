package nl.utwente.db.neogeo.scraper.srf;

import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.utils.NeoGeoProperties;
import nl.utwente.db.neogeo.utils.WebUtils;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class WebDriverSearchResultDetectionService implements SearchResultDetectionService {

	private String serviceUrl;
	
	public WebDriverSearchResultDetectionService() {
		init();
	}
	
	protected void init() {
		serviceUrl = NeoGeoProperties.getInstance().getProperty("searchresultdetectionservice.url");
	}
	
	public String detect(String url) {
		String serviceUrl = createServiceUrl(url);
		String json = WebUtils.getContent(createServiceUrl(url));
		
		try {
			JSONObject jsonObject = new JSONObject(json);
			JSONArray resultArray = (JSONArray)jsonObject.get("results");
			
			return (String)resultArray.get(0);
		} catch (JSONException e) {
			throw new ScraperException("Invalid JSON returned by " + serviceUrl, e);
		}
	}
	
	public static void main(String[] args) throws JSONException {
		WebDriverSearchResultDetectionService s = new WebDriverSearchResultDetectionService();

		System.out.println(s.detect("http://www.detelefoongids.nl/bg/plaats-enschede/w-restaurant/1/"));
	}

	private String createServiceUrl(String url) {
		return serviceUrl + url;
	}

	public String getServiceUrl() {
		return serviceUrl;
	}

	public void setServiceUrl(String serviceUrl) {
		this.serviceUrl = serviceUrl;
	}

}
