package nl.utwente.db.neogeo.scraper.utils;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import nl.utwente.db.neogeo.db.utils.HibernateUtils;
import nl.utwente.db.neogeo.scraper.model.ProxyServer;
import nl.utwente.db.neogeo.scraper.model.ScrapedPage;
import nl.utwente.db.neogeo.scraper.model.dao.ScrapedPageDAO;

import org.apache.log4j.Logger;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.WebWindow;

/**
 * The function of this class is to limit the number of requests issued to
 * a certain host.
 */
public class NeoGeoScraperWebClient extends WebClient {

	private static final long serialVersionUID = 1L;
	private static WebClient instance = new NeoGeoScraperWebClient();
	private static int DEFAULT_TIME_OUT = 10000;
	public static int NUMBER_CACHED_PAGES_BEFORE_COMMIT = 25;
	protected static int cachedPagesSinceLastCommit = 0;

	private Logger logger = Logger.getLogger(NeoGeoScraperWebClient.class);
	private ProxyServer proxyServer;
	private boolean cachingEnabled = true;

	private NeoGeoScraperWebClient() {
		super(BrowserVersion.FIREFOX_3_6);

		this.setCssEnabled(false);
		this.setJavaScriptEnabled(false);
		this.setAppletEnabled(false);
		
		this.setTimeout(DEFAULT_TIME_OUT);
	}
	
	public NeoGeoScraperWebClient(boolean cssEnabled, boolean javaScriptEnabled, boolean appletsEnabled) {
		super(BrowserVersion.FIREFOX_3_6);

		this.setCssEnabled(cssEnabled);
		this.setJavaScriptEnabled(javaScriptEnabled);
		this.setAppletEnabled(appletsEnabled);
		
		this.setTimeout(DEFAULT_TIME_OUT);
	}

	public NeoGeoScraperWebClient(ProxyServer proxyServer) {
		super(BrowserVersion.FIREFOX_3_6, proxyServer.getName().replace("http://", ""), proxyServer.getPort());
		
		this.proxyServer = proxyServer;

		this.setCssEnabled(false);
		this.setJavaScriptEnabled(false);
		this.setAppletEnabled(false);
		
		this.setTimeout(DEFAULT_TIME_OUT);
	}

	public <P extends Page> P getPage(String url, boolean accessCache, boolean storeInCache) throws IOException, FailingHttpStatusCodeException {
		return getPage(getCurrentWindow(), new WebRequest(new URL(url)), accessCache, storeInCache);
	}
	
	public <P extends Page> P getPage(WebRequest webRequest, boolean accessCache, boolean storeInCache) throws IOException, FailingHttpStatusCodeException {
		return getPage(getCurrentWindow(), webRequest, accessCache, storeInCache);
	}
	
	@Override
	public <P extends Page> P getPage(WebWindow webWindow, WebRequest webRequest) throws IOException, FailingHttpStatusCodeException {
		return getPage(webWindow, webRequest, cachingEnabled, cachingEnabled);
	}
	
	@SuppressWarnings("unchecked")
	public <P extends Page> P getPage(WebWindow webWindow, WebRequest webRequest, boolean accessCache, boolean storeInCache) throws IOException, FailingHttpStatusCodeException {
		logger.debug("Entering getPage for URL: " + webRequest.getUrl());

		if (!accessCache && !storeInCache) {
			logger.debug("Caching disabled: super.getPage");
			return 	super.getPage(webWindow, webRequest);
		}
		
		ScrapedPage scrapedPage = null;
		
		if (accessCache) {
			logger.debug("Looking for this page in the database");
			scrapedPage = getScrapedPageFromDatabase(webRequest);
			logger.debug("Cache result: " + scrapedPage);
			
			// Session cache can take up too much memory
			commitIfNecessary();
		}
		
		if (scrapedPage == null) {
			// Cache miss or disabled: get the page from the website
			long startTime = System.currentTimeMillis();
			
			String content = super.getPage(webWindow, webRequest).getWebResponse().getContentAsString();
			
			logger.debug("Executed super.getPage in " + (System.currentTimeMillis() - startTime) + "ms");

			scrapedPage = createScrapedPage(webRequest, content);
			
			if (storeInCache) {
				ScrapedPageDAO scrapedPageDao = new ScrapedPageDAO();
				scrapedPageDao.makePersistent(scrapedPage);
				logger.debug("Stored new page in database");
			}
			
			commitIfNecessary();
		}

		return (P)ScraperUtils.toHtmlPage(scrapedPage);
	}
	
	protected void commitIfNecessary() {
		if (++cachedPagesSinceLastCommit >= NUMBER_CACHED_PAGES_BEFORE_COMMIT) {
			HibernateUtils.commit();
			
			cachedPagesSinceLastCommit = 0;
		}
	}
	
	public ScrapedPage createScrapedPage(WebRequest webRequest, String content) {
		ScrapedPage scrapedPage = new ScrapedPage();
		
		scrapedPage.setUri(webRequest.getUrl().toString());
		scrapedPage.setMethod(webRequest.getHttpMethod().toString());
		scrapedPage.setParams(webRequest.getRequestParameters().toString());
		scrapedPage.setSource(webRequest.getUrl().getHost());
		scrapedPage.setName(scrapedPage.getUri());
		scrapedPage.setContent(content);

		return scrapedPage;
	}

	public ScrapedPage getScrapedPageFromDatabase(WebRequest webRequest) {
		List<ScrapedPage> results = new ArrayList<ScrapedPage>();
		ScrapedPageDAO scrapedPageDao = new ScrapedPageDAO();
		ScrapedPage scrapedPage = new ScrapedPage();
		
		scrapedPage.setUri(webRequest.getUrl().toString());
		scrapedPage.setMethod(webRequest.getHttpMethod().toString());
		scrapedPage.setParams(webRequest.getRequestParameters().toString());
		
		results = scrapedPageDao.findByExample(scrapedPage);
		
		if (results.isEmpty()) {
			return null;
		} else {
			return results.get(0);
		}
	}

	public static WebClient getInstance() {
		return instance;
	}

	public static void setInstance(WebClient instance) {
		NeoGeoScraperWebClient.instance = instance;
	}

	public ProxyServer getProxyServer() {
		return proxyServer;
	}

	public void setProxyServer(ProxyServer proxyServer) {
		this.proxyServer = proxyServer;
	}

	public boolean isCachingEnabled() {
		return cachingEnabled;
	}

	public void setCachingEnabled(boolean cachingEnabled) {
		this.cachingEnabled = cachingEnabled;
	}

}