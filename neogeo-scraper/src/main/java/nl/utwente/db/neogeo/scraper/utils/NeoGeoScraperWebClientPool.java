package nl.utwente.db.neogeo.scraper.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import nl.utwente.db.neogeo.db.hibernate.BaseModelObjectDAO;
import nl.utwente.db.neogeo.db.hibernate.GenericDAO;
import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.scraper.model.ProxyServer;
import nl.utwente.db.neogeo.utils.BasePool;

import org.apache.log4j.Logger;

public class NeoGeoScraperWebClientPool extends BasePool<NeoGeoScraperWebClient> {
	private static final long serialVersionUID = 1L;
	private static Logger logger = Logger.getLogger(NeoGeoScraperWebClientPool.class);
	protected static Map<String, NeoGeoScraperWebClientPool> hostnamePoolMap = new HashMap<String, NeoGeoScraperWebClientPool>();
	protected static NeoGeoScraperWebClientPool completePool;
	
	static {
		init();
	}
	
	public static void init() {
		completePool = new NeoGeoScraperWebClientPool();
		
		GenericDAO<ProxyServer> proxyServerDAO = new BaseModelObjectDAO<ProxyServer>(ProxyServer.class);
		List<ProxyServer> proxyServers = proxyServerDAO.findAll();
		
		logger.info("Loading " + proxyServers.size() + " proxyServers");
		
		for (ProxyServer proxyServer : proxyServers) {
			logger.debug("Loading proxy server " + proxyServer);
			completePool.add(new NeoGeoScraperWebClient(proxyServer));
		}
		
		if (completePool.isEmpty()) {
			throw new ScraperException("Proxy pool is empty. Please check your database connection.");
		}
	}
	
	public static NeoGeoScraperWebClientPool getInstance(String hostName) {
		if (hostnamePoolMap.containsKey(hostName)) {
			return hostnamePoolMap.get(hostName);
		}
		
		NeoGeoScraperWebClientPool result = (NeoGeoScraperWebClientPool)completePool.clone();
		hostnamePoolMap.put(hostName, result);
		
		return result;
	}
	
	public void refresh() {
		if (this.equals(completePool)) {
			return;
		}
		
		this.clear();
		
		for (NeoGeoScraperWebClient client : completePool) {
			this.add(client);
		}
	}

}