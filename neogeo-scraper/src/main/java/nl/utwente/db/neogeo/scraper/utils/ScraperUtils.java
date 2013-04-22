package nl.utwente.db.neogeo.scraper.utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.core.model.PointOfInterest;
import nl.utwente.db.neogeo.db.daos.PointOfInterestDAO;
import nl.utwente.db.neogeo.db.utils.HibernateUtils;
import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.scraper.ScrapingNotAllowedException;
import nl.utwente.db.neogeo.scraper.model.ScrapedPage;
import nl.utwente.db.neogeo.scraper.robotstxt.RobotsTxt;
import nl.utwente.db.neogeo.utils.CollectionUtils;

import org.apache.http.client.methods.HttpUriRequest;
import org.apache.log4j.Logger;
import org.w3c.dom.Node;

import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.StringWebResponse;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebRequest;
import com.gargoylesoftware.htmlunit.WebResponse;
import com.gargoylesoftware.htmlunit.html.DomNode;
import com.gargoylesoftware.htmlunit.html.HTMLParser;
import com.gargoylesoftware.htmlunit.html.HtmlDefinitionList;
import com.gargoylesoftware.htmlunit.html.HtmlDivision;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlTable;
import com.gargoylesoftware.htmlunit.html.HtmlTableBody;
import com.gargoylesoftware.htmlunit.util.UrlUtils;

public abstract class ScraperUtils {
	private static final Logger LOGGER = Logger.getLogger(ScraperUtils.class);
	public static int MAX_NR_ATTEMPTS_PER_PAGE = 3;

	/**
	 * Checks if this URL has been scraped during earlier sessions.
	 * @param linkedURL
	 * @return
	 */
	public static boolean hasBeenScraped(String linkedURL) {
		PointOfInterestDAO poiDao = new PointOfInterestDAO();
		HibernateUtils.getTransaction();
		
		PointOfInterest poiTemplate = new PointOfInterest();
		poiTemplate.setSourceUrl(linkedURL);
		
		return !poiDao.findByExample(poiTemplate).isEmpty();
	}

	public static WebRequest toWebRequest(HttpUriRequest request) {
		WebRequest webRequest;

		if (!request.getMethod().toUpperCase().equals("GET")) {
			// TODO fix this
			throw new NeoGeoException("This only works for GETs so far. Please improve the implementation.");
		}

		try {
			webRequest = new WebRequest(request.getURI().toURL());
		} catch (MalformedURLException e) {
			throw new NeoGeoException("Unable to transform HttpUriRequest to WebRequest", e);
		}

		return webRequest ;
	}

	public static NeoGeoScraperWebClientPool getWebClientPool(WebRequest webRequest) {
		return getWebClientPool(webRequest.getUrl());
	}
	
	public static NeoGeoScraperWebClientPool getWebClientPool(URL url) {
		return getWebClientPool(url.getHost());
	}
	
	public static NeoGeoScraperWebClientPool getWebClientPool(String hostName) {
		return NeoGeoScraperWebClientPool.getInstance(hostName);
	}
	
	public static NeoGeoScraperWebClient getRandomWebClient(URL url) {
		return getWebClientPool(url).getRandomItem();
	}

	public static HtmlPage toHtmlPage(String url) {
		WebRequest request = createWebRequest(url);
		
		return toHtmlPage(request);
	}
	
	public static HtmlPage toHtmlPage(WebRequest request) {
		return toHtmlPage(request, 0);
	}
	
	public static HtmlPage toHtmlPage(WebRequest request, int nrAttempts) {
		String url = request.getUrl().toString();

		if (!url.endsWith("robots.txt") && !RobotsTxt.isScrapingAllowed(url)) {
			throw new ScrapingNotAllowedException("Scraping of this url is prohibited by the robots.txt of this webpage");
		}
		
		if (nrAttempts >= MAX_NR_ATTEMPTS_PER_PAGE) {
			throw new ScraperException("Maximum number of attempts per page has been reached for " + request);
		}
		
		LOGGER.info("Opening " + request.getUrl() + " ...");
		NeoGeoScraperWebClientPool pool = getWebClientPool(request);
		
		if (pool.isEmpty()) {
			pool.refresh();
		}
		
		WebClient webClient = pool.getRandomItem();

		try {
			return webClient.getPage(request);
		} catch (FailingHttpStatusCodeException e) {
			if (e.getStatusCode() != 500) {
				// Don't blame the proxy for internal server errors
				pool.remove(webClient);
			}

			LOGGER.error("FailingHttpStatusCodeException while requesting " + request + ". Will attempt with another proxy. " + pool.size() + " proxies left for this hostname.",  e);
			
			// Recursion to a limited depth
			return toHtmlPage(request, ++nrAttempts);
		} catch (IOException e) {
			pool.remove(webClient);
			LOGGER.error("IOException while requesting " + request + ". Will attempt with another proxy. " + pool.size() + " proxies left for this hostname.",  e);
			
			// Recursion to a limited depth
			return toHtmlPage(request, ++nrAttempts);
		}
	}

	public static WebRequest createWebRequest(String url) {
		URL urlObject;

		try {
			urlObject = new URL(url);
		} catch (MalformedURLException e) {
			throw new NeoGeoException("Unable to parse url " + url, e);
		}

		return new WebRequest(urlObject);
	}
	
	public static List<String> getClassNames(DomNode domNode) {
		Node classAttributeNode = domNode.getAttributes().getNamedItem("class");
		List<String> classNames = new ArrayList<String>();

		if (classAttributeNode != null) {
			String classNamesString = classAttributeNode.getNodeValue();
			classNames = Arrays.asList(classNamesString.split(" "));
		}

		return classNames;
	}

	public static boolean isTableOrTableBody(HtmlElement htmlElement) {
		return htmlElement instanceof HtmlTable || htmlElement instanceof HtmlTableBody;
	}

	public static boolean isDefinitionList(HtmlElement htmlElement) {
		return htmlElement instanceof HtmlDefinitionList;
	}

	public static boolean isTwoColumnDivGrid(HtmlElement htmlElement) {
		boolean result = false;
		
		for (HtmlElement childElement : htmlElement.getChildElements()) {
			if (!(childElement instanceof HtmlDivision)) {
				return false;
			}
			
			// Skip to first non-styling div
			childElement = getFirstNonTrivialDiv((HtmlDivision)childElement);
			Iterable<HtmlElement> grandChildElements = childElement.getChildElements();
			
			@SuppressWarnings("unchecked")
			List<HtmlElement> grandChildElementsList = CollectionUtils.asList(grandChildElements);

			if (grandChildElementsList.size() != 2) {
				return false;
			}
			
			for (HtmlElement grandChildElement : grandChildElements) {
				if (!(grandChildElement instanceof HtmlDivision)) {
					return false;
				}
			}
			
			result = true;
		}
		
		return result;
	}
	
	/**
	 * HtmlDivisions can be nested for styling purposes, this method returns the first element containing more
	 * than just another HtmlDivision
	 */
	public static HtmlDivision getFirstNonTrivialDiv(HtmlDivision htmlElement) {
		Iterable<HtmlElement> childElements = htmlElement.getChildElements();
		
		@SuppressWarnings("unchecked")
		List<HtmlElement> childElementsList = CollectionUtils.asList(childElements);

		if (childElementsList.size() != 1 || !(childElementsList.get(0) instanceof HtmlDivision)) {
			return htmlElement;
		} else {
			return getFirstNonTrivialDiv((HtmlDivision)childElementsList.get(0));
		}
	}

	public static HtmlPage toHtmlPage(String url, String htmlContent) {
		URL urlObject = UrlUtils.toUrlSafe(url);
		StringWebResponse response = new StringWebResponse(htmlContent, urlObject);

		HtmlPage page = null;

		try {
			page = HTMLParser.parseHtml(response, ScraperUtils.getRandomWebClient(urlObject).getCurrentWindow());
		} catch (IOException e) {
			throw new ScraperException("IOException while parsing HTML response" + response);
		}
		
		return page;
	}
	
	public static HtmlPage toHtmlPage(ScrapedPage scrapedPage) {
		return toHtmlPage(scrapedPage.getUri(), scrapedPage.getContent());
	}

	public static String getContent(String url) {
		WebRequest request = createWebRequest(url);
		
		return getContent(request);
	}
	
	public static String getContent(WebRequest request) {
		return getContent(request, 0);
	}
	
	public static String getContent(WebRequest request, int nrAttempts) {
		NeoGeoScraperWebClientPool pool = getWebClientPool(request);
		
		if (pool.isEmpty()) {
			pool.refresh();
		}
		
		WebClient webClient = pool.getRandomItem();
		return getContent(webClient, request, nrAttempts);
	}
	
	public static String getContent(WebClient webClient, String url) {
		return getContent(webClient, createWebRequest(url), 0);
	}
	
	public static String getContent(WebClient webClient, String url, boolean checkRobotsTxt) {
		return getContent(webClient, createWebRequest(url), 0, checkRobotsTxt);
	}

	public static String getContent(WebClient webClient, WebRequest request, boolean checkRobotsTxt) {
		return getContent(webClient, request, 0, checkRobotsTxt);
	}
	
	public static String getContent(WebClient webClient, WebRequest request, int nrAttempts) {
		return getContent(webClient, request, nrAttempts, true);
	}
	
	public static String getContent(WebClient webClient, WebRequest request, int nrAttempts, boolean checkRobotsTxt) {
		String url = request.getUrl().toString();

		if (checkRobotsTxt && !url.endsWith("robots.txt") && !RobotsTxt.isScrapingAllowed(url)) {
			throw new ScrapingNotAllowedException("Scraping of this url is prohibited by the robots.txt of this webpage");
		}
		
		if (nrAttempts >= MAX_NR_ATTEMPTS_PER_PAGE) {
			throw new ScraperException("Maximum number of attempts per page has been reached for " + request);
		}
		
		LOGGER.info("Opening " + request.getUrl() + " ...");
		

		try {
			WebResponse webResponse = webClient.loadWebResponse(request);
			
			return webResponse.getContentAsString();
		} catch (Exception e) {
			if (e instanceof FailingHttpStatusCodeException && ((FailingHttpStatusCodeException)e).getStatusCode() != 500) {
				// Don't blame the proxy for internal server errors
				getWebClientPool(request).remove(webClient);
			}

			LOGGER.error("FailingHttpStatusCodeException while requesting " + request + ". Will attempt with another proxy. " + getWebClientPool(request).size() + " proxies left for this hostname.",  e);
			
			// Recursion to a limited depth
			return getContent(request, ++nrAttempts);
		}
	}
}