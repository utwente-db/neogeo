package nl.utwente.db.neogeo.scraper.robotstxt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.commons.lang.ArrayUtils;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.TextPage;


public class RobotsTxt {

	private static final boolean debug = false;
	
	static class HostRobotDirectives {
		
		String								host;
		String  							robotsTxt;
		Vector<UserAgentRobotDirectives>	agents;
		long 								timestamp;
		
		HostRobotDirectives(String host, String robotsTxt){
			if ( debug ) System.out.println("+ new RobotDirective("+host+") : ");
			// if ( debug ) System.out.println(robotsTxt);
			this.host 		= host;
			this.robotsTxt 	= robotsTxt;
			this.agents		= new Vector<UserAgentRobotDirectives>();
			this.timestamp 	= (new Date()).getTime();
			
			parseRobotsTxt();
		}	
		
		private void parseRobotsTxt() {
			String[] lines = robotsTxt.split("[\\n\\r]");
	        //first delete comments and empty lines
	        for (int i = 0; i < lines.length; i ++){
	        	String l = lines[i];
	        	if ( l.indexOf('#') >= 0 )
	        		l = l.substring(0, l.indexOf('#'));
	        	lines[i] =  l.trim();
	            if ( l.length() == 0 )
	                ArrayUtils.remove(lines, i);
	        }
	        int i, j;
	        
	        for (i = 0; i < lines.length; i++){
	        	// if (debug) System.out.println("<"+lines[i]+">");
	            if (lines[i].toLowerCase().startsWith("user-agent")){
	            	String	agentName = lines[i].substring(lines[i].indexOf(':')+1).trim();
	            	UserAgentRobotDirectives uad = new UserAgentRobotDirectives(this,agentName);
	            	agents.add(uad);
	                for (j = i + 1; j < lines.length; j++){
	                	// if (debug) System.out.println("<<"+lines[j]+">>");
	                    if (lines[j].toLowerCase().startsWith("disallow")){
	                    	uad.addDisallow(lines[j]);	                        
	                    } else if (lines[j].toLowerCase().startsWith("allow")) {
	                    	uad.addAllow(lines[j]);	     
	                    } else if (lines[j].toLowerCase().startsWith("user-agent")){
	                    	agentName = lines[j].substring(lines[j].indexOf(':')+1).trim();
	                    	if ( uad.empty ) {
	                    		// header of an existing user agent block, just a agent
	                    		uad.addAgent(agentName);
	                    	} else {
	                    		// a new user agent block beginning
	                    		uad = new UserAgentRobotDirectives(this,agentName);
	                    		break;
	                    	}
	                    } else if (lines[j].toLowerCase().startsWith("sitemap")) {
	                    	uad.addSitemap(lines[j]);	 
	                    } else if (lines[j].toLowerCase().startsWith("crawl-delay")) {
	                    	uad.addCrawlDelay(lines[j]);	 
	                    } else {
	                    	// System.err.println("RobotsTxt: unrecognized: "+lines[j]);
	                    }
	                }
	                i = j;
	            }
	        }
		}
		
		public UserAgentRobotDirectives bestUserAgentMatch(String userAgent) {
			UserAgentRobotDirectives res = null;
			
			for (int i=0; i<agents.size(); i++) {
				UserAgentRobotDirectives uad = agents.elementAt(i);
				
				if ( uad.matchesUserAgent(userAgent) ) {
					if ( res == null )
						res = uad;
					else if ( !uad.allAgents && res.allAgents )
						res = uad;
					else
						System.err.println("userAgent: multiple wildcards");
					if ( !uad.allAgents )
						return uad;
				}
			}
			if ( debug ) System.out.println("+ BestUser-Agent("+userAgent+")=\n"+res);
			return res;
		}
		
		
		public boolean scrapeAllowed(String userAgent, String path) {
			UserAgentRobotDirectives best = bestUserAgentMatch(userAgent);
			
			if ( path.length() == 0 )
				path = "/";
			if (best != null)
				return best.isAllowed(path);
			return true;
		}
		
		public String sitemap(String userAgent) {
			UserAgentRobotDirectives best = bestUserAgentMatch(userAgent);
			
			if ( best != null )
				return best.sitemap;
			else
				return null;
		}
		
		public int crawl_delay(String userAgent) {
			UserAgentRobotDirectives best = bestUserAgentMatch(userAgent);
			
			if ( best != null )
				return best.crawlDelay;
			else
				return 0;
		}
		
		public String toString() {
			StringBuilder res = new StringBuilder();
			
			res.append("# the robots.txt rebuild from site \'");
			res.append(host);
			res.append("\'\n\n");
			for(int i=0; i<agents.size(); i++) {
				res.append( agents.elementAt(i).toString());
				res.append('\n');
			}
			return res.toString();
		}
	}
	
	static class UserAgentRobotDirectives {
		HostRobotDirectives	hostDir;
		Vector<String>		agents;
		boolean				allAgents;
		boolean				empty;
		Vector<String>		allow;
		Vector<String>		disallow;
		String				sitemap;
		int					crawlDelay;

		UserAgentRobotDirectives(HostRobotDirectives hostDir, String userAgent) {
			if ( debug ) System.out.println("->"+hostDir.host+":userAgent: \""+userAgent+"\"");
			this.hostDir	= hostDir;
			this.agents		= new Vector<String>();
			allAgents		= false;
			addAgent(userAgent);
			this.empty		= true;
			allow			= new Vector<String>();
			disallow		= new Vector<String>();
			this.sitemap	= null;
			this.crawlDelay	= 0;
		}
		
		public void addAgent(String userAgent) {
			if ( userAgent.equals("*") ) {
				this.allAgents = true;
			}
			this.agents.add( userAgent );		
		}
		
		public boolean matchesUserAgent(String userAgent) {
			if ( allAgents )
				return true; // "*" is used
			else {
				for(int i=0; i<agents.size(); i++) 
					if ( agents.elementAt(i).equals(userAgent) )
						return true;
			}
			return false;
		}
		
		public boolean isAllowed(String url) {
			for (int i=0; i<disallow.size(); i++) {
				if ( url.startsWith( disallow.elementAt(i) )  ) {
					// element is not allowed unless specifically allowed by longer allow
					for (int j=0; j<allow.size(); j++ ) {
						/* when the url is allowed and the allow prefix is longer than the
						 * disallow prefix the url is allowed (bing semantics)
						 */
						if ( url.startsWith( allow.elementAt(j) ) && 
								( allow.elementAt(j).length() > disallow.elementAt(i).length() ) )
							return true;
					}
					return false;
				}
			}
			return true;
		}
		
		private String clearLine(String firstWord, String line) {
			if ( line.startsWith(firstWord))
				return line.substring(line.indexOf(':')+1).trim();
			else
				return line;
		}
		
		public void addAllow(String line) {
			this.empty = false;
			line = clearLine("Allow",line);
			allow.add( line);
			if ( debug ) System.out.println("\tAllow: \""+line+"\"");
		}
		
		public void addDisallow(String line) {
			this.empty = false;
			line = clearLine("Disallow",line);
			disallow.add(line);
			if ( debug ) System.out.println("\tDisallow: \""+line+"\"");
		}
		
		public void addSitemap(String line) {
			this.empty = false;
			line = clearLine("Sitemap",line);
			this.sitemap = line;
			if ( debug ) System.out.println("\tSitemap: \""+line+"\"");
		}
		
		public void addCrawlDelay(String line) {
			this.empty = false;
			line = clearLine("Crawl-delay",line);
			this.crawlDelay = Integer.parseInt(line);
			if ( debug ) System.out.println("\tCrawl-delay: \""+line+"\"");
		}
		
		public String toString() {
			StringBuilder res = new StringBuilder();
			int i;
			
			for(i=0; i<agents.size(); i++) {
				res.append("User-agent: ");
				res.append(agents.elementAt(i));
				res.append('\n');
			}
			for(i=0; i<allow.size(); i++) {
				res.append("Allow: ");
				res.append(allow.elementAt(i));
				res.append('\n');
			}
			for(i=0; i<disallow.size(); i++) {
				res.append("Disallow: ");
				res.append(disallow.elementAt(i));
				res.append('\n');
			}
			if ( crawlDelay > 0 ) {
				res.append("Crawl-delay: "+ crawlDelay + "\n");
			}
			if ( sitemap != null ) {
				res.append("Sitemap: "+ sitemap + "\n");
			}
			return res.toString();
		}
	}
	
	private	static long	cacheDuration = 10000000; // time in ms. the directives are cached
	
	private static Hashtable<String,HostRobotDirectives>	robotHosts = new Hashtable<String,HostRobotDirectives>();
	
	public static void clearCache() {
		robotHosts = new Hashtable<String,HostRobotDirectives>();
	}
	
	public static void setCacheDuration(long ms) {
		cacheDuration = ms;
	}
	
	private static HostRobotDirectives getHostDirectives(WebClient wc, URL url) {
		HostRobotDirectives	res = findHostDirectives(url.getHost());
		if ( res == null ) {
			String robotTxt = getRobotsTxt(wc, url.getHost());
			if ( robotTxt == null )
				return null; // an error ocurred
			else {
				res = new HostRobotDirectives(url.getHost(), robotTxt);
				robotHosts.put(url.getHost(), res);
			}
		}
		return res;
	}

	private static HostRobotDirectives findHostDirectives(String host) {
		HostRobotDirectives	res = robotHosts.get(host);
		
		if ( res != null ) {
			if ( ((new Date()).getTime() - res.timestamp) > cacheDuration ) {
				robotHosts.remove(host); // timestamp exceeeded duration
				res = null;
			}
		}
		return res;
	}
	
	public static boolean isScrapingAllowed(String str_url) {
		return isScrapingAllowed(null,str_url,null);
	}
	
	public static boolean isScrapingAllowed(WebClient wc, String str_url,
			String userAgent) {
		boolean res = true;
		if ( userAgent == null )
			userAgent = "*";
		try {
			URL url = new URL(str_url);
			HostRobotDirectives agentDirectives = getHostDirectives(wc, url);

			if ( debug ) System.out.println("HostDirectives("+url.getHost()+")=\n" + agentDirectives);
			if (agentDirectives != null)
				res = agentDirectives.scrapeAllowed(userAgent, url.getPath());
			if ( debug ) System.out.println("isScrapingAllowed("+ userAgent + "," + str_url+")="+res);
			return res;
		} catch (MalformedURLException e) {
			System.err.println("isScrapingAllowed: "+e); // incomplete															// ugly
			return false; // incomplete. an error ocurred
		}
	}
	
	public static String sitemap(WebClient wc, String str_url, String userAgent) {
		if ( userAgent == null )
			userAgent = "*";
		try {
			URL url = new URL(str_url);
			HostRobotDirectives agentDirectives = getHostDirectives(wc, url);

			if (agentDirectives != null)
				return agentDirectives.sitemap(userAgent);
		} catch (MalformedURLException e) {
			System.err.println("sitemap: "+e); // incomplete															// ugly
			return null; // incomplete. an error ocurred
		}
		return null;
	}
	
	public static int crawl_delay(WebClient wc, String str_url, String userAgent) {
		if ( userAgent == null )
			userAgent = "*";
		try {
			URL url = new URL(str_url);
			HostRobotDirectives agentDirectives = getHostDirectives(wc, url);

			if (agentDirectives != null)
				return agentDirectives.crawl_delay(userAgent);
		} catch (MalformedURLException e) {
			System.err.println("crawl_delay: "+e); // incomplete															// ugly
			return 0; // incomplete. an error ocurred
		}
		return 0;
	}

	private static String getRobotsTxt(WebClient wc, String host) {
		String robotstxt_url = "http://" + host + "/robots.txt";

		if (wc != null) {
			try {
				return readRobotsTxt(wc, robotstxt_url);
			} catch (IOException e) {
				return null; // there is no robots.txt here
			}
		} else {
			try {
				StringBuilder res = new StringBuilder();
				URL url = new URL(robotstxt_url);

				BufferedReader in = new BufferedReader(new InputStreamReader(
						url.openStream()));
				String str;
				while ((str = in.readLine()) != null) {
					res.append(str);
					res.append('\n');
				}
				in.close();
				return res.toString();
			} catch (IOException e) {
				System.err.println("Exception: " + e);
				return null;
			}
		}
	}
	
	private static String readRobotsTxt(WebClient wc, String robotstxt_url)
			throws IOException {
		try {
			TextPage tp = (TextPage) wc.getPage(robotstxt_url);
			if (tp != null) {
				return tp.getContent();
			}
		} catch (com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException e) {
			return ""; // no robots.txt
		}
		return "";
	}
	
}
