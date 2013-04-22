package nl.utwente.db.neogeo.scraper.srf;

import nl.utwente.db.neogeo.scraper.robotstxt.*;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

import com.gargoylesoftware.htmlunit.WebClient;

public class SrfTester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// String kind = "single";
		// String kind = "directory";
		// String kind = "url";
		// String kind = "robots.txt";
		String kind = "robust";
		
		if ( kind.equals("robots.txt")) {
			// RobotsTxt.isScrapingAllowed(null ,"http://www.utwente.nl/china/", "NeoBot");
			RobotsTxt.isScrapingAllowed(new WebClient(),"http://www.utwente.nl/should-be-allowed/", "NeoBot");
			// RobotsTxt.isScrapingAllowed(new WebClient(),"http://www.springer.com", "NeoBot");
			// RobotsTxt.isScrapingAllowed(new WebClient(),"http://www.springer.com", "Googlebot");

		} else if (kind.equals("single")) {
			// String url = "http://www.detelefoongids.nl/bg/plaats-enschede/w-tandarts/1/";
			String url = "http://www.detelefoongids.nl/zoeken/plaats-enschede/w-restaurant";
			Vector<String> terms = new Vector<String>();
			runUrlTest(url, terms);
		} else if (kind.equals("directory")) {
			String dirName = "/Users/flokstra/SRFTEST/by-dir";
			String select[] = null;
			// String select[] = { "adobe_cows" }; // only a selection
			runDirectoryTest(dirName, select);
		} else if (kind.equals("url")) {
			String fileName = "/Users/flokstra/SRFTEST/by-url/url-list";
			runFileTest(fileName);
		} else if (kind.equals("robust")) {			
			String url = "http://www.detelefoongids.nl/zoeken/plaats-enschede/w-restaurant";
			Vector<String> res = SearchResultFinder.robustSRF(url);
			System.out.println("robust-res: "+url+":");
			for(int i=0; i<res.size(); i++) 
				System.out.println(res.elementAt(i));
		}
	}

	/*
	 * Automated test function for SearchResultFinder. Test setup is very simple:
	 * - parameter is the test directory
	 * - every directory name in test directory without "." is expected to contain an index.html to be tested
	 * - a <dirname>.srf file is generated with the complete details of the test
	 */
	public static void runDirectoryTest(String dirName, String select[]) {
		SearchResultFinder srf = new SearchResultFinder();
		srf.setJavaScriptEnabled(true);
		srf.ignoreAllErrors();

		System.out.println("Starting Srf test, dir="+dirName + "\n");
		String files[];
		if ( select != null && (select.length > 0) )
			files = select;
		else {
			File dir = new File(dirName);
			files = dir.list(); // select all files in directory
		}
		for (int i = 0; i < files.length; i++) {
			if (files[i].indexOf('.') == -1) {
				String base_url = dirName + File.separator + files[i];
				testPage(srf,"file://"+base_url+"/index.html", base_url+".srf");
			}
		}
		srf.close();
		System.out.println("\nFinish Srf test.");
	}
	
	public static boolean runFileTest(String fileName) {
		SearchResultFinder srf = new SearchResultFinder();
		srf.setJavaScriptEnabled(true);
		srf.ignoreAllErrors();

		System.out.println("Starting Srf test, url-file="+ fileName + "\n");
		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));

			String url;
			while ((url = in.readLine()) != null) {
				System.out.println("URL=" + url);
				testPage(srf, url, null);
			}
			in.close();
		} catch (IOException e) {
			System.out.println("#!Caught: " + e);
			return false;
		}
		srf.close();
		System.out.println("\nFinish Srf test.");
		return true;
	}

	public static void testPage(SearchResultFinder srf, String url, String logfile) {
		Vector<CandidatePredicate> res;
		StringBuilder sb = new StringBuilder();
		
		System.out.println("- Analyzing: "+url);
		try {
			res = srf.analyzePage(url, null);
		} catch (Exception e) {
			System.out.println("x exception: " + e);
			return;
		}
		if ( res == null )
			System.out.println("(null) operation failed.");
		else if (res.size() > 0) {
			sb.append("* SearchResultFinders results for: " + url + "\n");
			for (int i = 0; i < res.size(); i++) {
				System.out.println(res.elementAt(i).explain());
				sb.append(res.elementAt(i).explain()+"\n");
				sb.append("===== explain_all ===========\n");
				sb.append(srf.explain_all() + "\n");
			}
			if ( true ) {
				srf.renderServer.highlight( res.elementAt(0).xpath() );
				if ( true )
					mySleep(8000);
			}
		} else {
			sb.append("* SearchResultFinders NO results for: " + url +"\n");
			sb.append(srf.explain_all() + "\n");
		}
		if ( srf.ignoredErrors() )
			sb.append("#!IGNORED ERRORS ARE:\n"+srf.ignoredErrorList()+"\n");
		if (logfile != null )
			string2file(sb.toString(),logfile);
	}
	
	public static void runUrlTest(String url, Vector<String> terms) {
		SearchResultFinder srf = new SearchResultFinder();

		Vector<CandidatePredicate> res;

		srf.setJavaScriptEnabled(true);
		srf.ignoreAllErrors();
		res = srf.analyzePage(url, terms);
		if ( (res!=null) && res.size() > 0) {
			System.out.println("* SearchResultFinders results for: " + url);
			if ( true ) {
				srf.renderServer.highlight( res.elementAt(0).xpath() );
				if ( true )
					mySleep(3000);
			}
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
	
	/*
	 * Some utilities
	 */
	
	private static boolean string2file(String s, String file) {
	    try {
	        FileWriter out = new FileWriter(file);
	        out.write(s,0,s.length());
	        out.close();
	        return true;
	    } 
	    catch (IOException e) {
	      System.out.println("#!Caught: "+e);
	      return false;
	    }
	  }
	
	  public static void mySleep(int msec) {
		    try{
		      Thread.currentThread().sleep(msec);
		    } catch(InterruptedException ie){
		    }
	  }

}
