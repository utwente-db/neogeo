package nl.utwente.db.neogeo.scraper.test.proxy;

import nl.utwente.db.neogeo.db.hibernate.BaseModelObjectDAO;
import nl.utwente.db.neogeo.db.hibernate.GenericDAO;
import nl.utwente.db.neogeo.db.utils.HibernateUtils;
import nl.utwente.db.neogeo.scraper.ScraperException;
import nl.utwente.db.neogeo.scraper.model.ProxyServer;
import nl.utwente.db.neogeo.scraper.utils.NeoGeoScraperWebClient;
import nl.utwente.db.neogeo.scraper.utils.NeoGeoScraperWebClientPool;
import nl.utwente.db.neogeo.utils.WebUtils;
import nl.utwente.db.neogeo.utils.tasks.MonitoredRunnable;
import nl.utwente.db.neogeo.utils.test.TestResult;

import org.apache.log4j.Logger;

import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class ProxyTestRunnable extends MonitoredRunnable<TestResult> {
	private Logger logger = Logger.getLogger(ProxyTestRunnable.class);
	private final String testAddress;
	private boolean stopped = false;
	
	private int nrTestedProxies = 0;
	private int poolSize = 1; // Avoid division by zero
	
	public ProxyTestRunnable(String testAddress) {
		this.testAddress  = testAddress;
	}
	
	public void run() {
		String hostName = WebUtils.getHostName(testAddress);
		NeoGeoScraperWebClientPool pool = NeoGeoScraperWebClientPool.getInstance(hostName);
		
		TestResult initiationResult = new TestResult();
		
		initiationResult.setName("Proxy pool initiated");
		initiationResult.setSuccess(pool.size() > 0);
		initiationResult.setDescription("Pool size: " + pool.size());
		
		addUpdate(initiationResult);
		
		poolSize = pool.size();
		
		GenericDAO<ProxyServer> proxyServerDao = new BaseModelObjectDAO<ProxyServer>(ProxyServer.class);

		int i = 0;
		
		for (NeoGeoScraperWebClient client : pool) {
			if (this.stopped) {
				break;
			}
			
			String proxyHostName = client.getProxyConfig().getProxyHost();
			TestResult testResult = new TestResult();
			
			logger.debug("Testing proxy: " + proxyHostName);
			
			testResult.setName(proxyHostName);
			
			try {
				logger.debug("getting Page " + i++);
				HtmlPage page = client.getPage(testAddress, false, false);
				logger.debug("got Page");
				
				if (!"\"Bakkerij Meinders Echte Bakker\", \"Wierden\", Bakkerijen - De Telefoongids Bedrijvengids".equals(page.getTitleText().trim())) {
					throw new ScraperException("Client did not return the expected title.");
				}
				
				testResult.setSuccess(true);
				testResult.setDescription(page.getTitleText());
			} catch (Exception e) {
				logger.error("Proxy failed: " + proxyHostName, e);
				
				proxyServerDao.makeTransient(client.getProxyServer());
				HibernateUtils.commit();
				
				testResult.setSuccess(false);
				testResult.setDescription(e.getClass().getName() + ": " + e.getMessage());
			}
			
			logger.info("Test result for " + proxyHostName + ": " + testResult);
			
			addUpdate(testResult);
			nrTestedProxies++;
		}
	}
	
	public void stop() {
		this.stopped  = true;
	}


	@Override
	public double getProgress() {
		return ((double)nrTestedProxies / poolSize);
	}
}
