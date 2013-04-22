package nl.utwente.db.neogeo.pubs;

import java.util.List;
import java.util.Vector;

import com.gargoylesoftware.htmlunit.html.*;
import com.gargoylesoftware.htmlunit.WebClient;

import nl.utwente.db.neogeo.db.utils.HibernateUtils;
import org.hibernate.Session;

import org.hibernate.jdbc.Work;
import java.sql.*;

import nl.utwente.db.neogeo.scraper.robotstxt.*;

import nl.utwente.db.neogeo.scraper.srf.*;

public class HelloWorld {
	
	public static void main(String[] argv) {
		System.out.println("Running my first HtmlUnit");
		// testHU("http://htmlunit.sourceforge.net");
		
		navigateMVK("http://wwwhome.ewi.utwente.nl/~keulen/wordpress/");
		// runSRF("http://www.lonelyplanet.com/searchResult?q=nepal",new Vector<String>());
	}	

	public static synchronized Connection get_hibernateJDBC() {		
		final Connection ac[] = new Connection[1];
		
		Session s = HibernateUtils.getSession();
		if ( !s.getTransaction().isActive() )
			s.getTransaction().begin();
		if ( !s.isConnected() )
			System.out.println("#!Session is not connected");

		s.doWork(new Work() {
		    public void execute(Connection connection) throws SQLException {
		    	ac[0] = connection;
		    }
		});
		return ac[0];
	}
	
	private static Connection last_connection = null;

	public static synchronized Connection hibernateJDBC() throws SQLException {	
		if ( last_connection == null || last_connection.isClosed() )
			last_connection = get_hibernateJDBC();
		return last_connection;
	}

	
	public static void testHU(String url_str) {
		final WebClient webClient = new WebClient();
		HtmlPage page = null;
		
		if ( RobotsTxt.isScrapingAllowed(url_str) )
			System.out.println("Scraping: "+url_str + " is allowed");
		else
			System.out.println("Scraping: "+url_str + " is NOT allowed");
		try {
	       page = webClient.getPage(url_str);
	       System.out.println("Title="+page.getTitleText());
	       
	       List<HtmlElement> l = (List<HtmlElement>)page.getByXPath("//a");
	       for(int i=0; i<l.size(); i++) {
	    	   HtmlElement he = l.get(i);
	    	   System.out.println("* HtmlElement:");
	    	   System.out.println("- XPath: " + he.getCanonicalXPath());
	    	   System.out.println("- attr: "+he.getAttributes());
	    	   System.out.println("- text: " + he);
	       }
	       
	       
		} catch(Exception e) {
		   System.out.println("Caught: "+e);
		}
	  
	    final String pageAsText = page.asText();
	    System.out.println("CONTENTS="+pageAsText.contains("Support for the HTTP and HTTPS protocols"));

	    webClient.closeAllWindows();
	
	}
	
	public static void navigateMVK(String url_str) {
		final WebClient webClient = new WebClient();
		HtmlPage page = null;
		
		if ( RobotsTxt.isScrapingAllowed(webClient, url_str, "*") )
			System.out.println("Scraping: "+url_str + " is allowed");
		else
			System.out.println("Scraping: "+url_str + " is NOT allowed");
		try {
	       page = webClient.getPage(url_str);
	       System.out.println("Title="+page.getTitleText());
	       
	       @SuppressWarnings("unchecked")
		List<HtmlElement> l = (List<HtmlElement>)page.getByXPath("//a");
	       System.out.println("Anchors: #="+l.size());
//	       if ( false ) for(int i=0; i<l.size(); i++) {
//	    	   HtmlElement he = l.get(i);
//	    	   System.out.println("* HtmlElement:");
//	    	   System.out.println("- XPath: " + he.getCanonicalXPath());
//	    	   System.out.println("- attr: "+he.getAttributes());
//	    	   System.out.println("- text: " + he);
//	       }
	       final String pageAsText = page.asText();
		   System.out.println("Contains(Neogeography)="+pageAsText.contains("Neogeography"));
	       HtmlAnchor a = page.getAnchorByText("Neogeography");
	       if ( a == null ) 
	    	   System.out.println("Neogeography: not found");
	       else {
	    	   System.out.println("Neogeography: "+a);
	    	   System.out.println("XPath for Anchor: " + a.getCanonicalXPath());
	    	   System.out.println("Follow link");
	    	   HtmlPage newpage = a.click();
	    	   System.out.println("New Title="+newpage.getTitleText());
	       }
		} catch(Exception e) {
		   System.out.println("Caught: "+e);
		}
	    webClient.closeAllWindows();
	}
	
	public static void runSRF(String url, Vector<String> terms) {
		SearchResultFinder srf = new SearchResultFinder();

		Vector<CandidatePredicate> res;

		srf.setJavaScriptEnabled(true);
		srf.ignoreAllErrors();
		res = srf.analyzePage(url, terms);
		if ( (res!=null) && res.size() > 0) {
			System.out.println("* SearchResultFinders results for: " + url);
			for (int i = 0; i < res.size(); i++) {
				// System.out.println("+ "+res.elementAt(i).xpath()+" = "+res.elementAt(i).score());
				System.out.println(res.elementAt(i).explain());
			}
			if (true) {
				System.out
						.println("============= ALL ======================\n");
				System.out.println(srf.explain_all());
				System.out
						.println("======== BEST HTMLUNIT NODES ===========\n");
				CandidatePredicate best = res.elementAt(0);
				for (int i = 0; i < best.resultnode.size(); i++) {
					System.out.println(best.resultnode.get(i).asText());
					System.out.println("<<<<<<<<<<<<<<<<<<");
				}
			}
		} else {
			System.out.println("* SearchResultFinders NO results for: " + url);
			if ( res == null )
				System.out.println("(null) result!");
			else
				System.out.println(srf.explain_all());
		}
		if ( srf.ignoredErrors() )
			System.out.println("\n#!IGNORED ERRORS ARE:\n"+srf.ignoredErrorList());
		srf.close();
	}
	
}
