package nl.utwente.db.neogeo.scraper.srf;

import java.util.List;
import java.util.Vector;
import java.util.Hashtable;
import java.util.Iterator;

import nl.utwente.db.neogeo.scraper.utils.ScraperUtils;

import org.basex.util.hash.IntSet;
import org.w3c.dom.Node;

import com.gargoylesoftware.htmlunit.html.*;
import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;

import java.net.MalformedURLException;
import java.net.URL;
import com.gargoylesoftware.htmlunit.IncorrectnessListener;
import com.gargoylesoftware.htmlunit.javascript.JavaScriptErrorListener;
import com.gargoylesoftware.htmlunit.ScriptException;


/*
 * Problems too solve:
 * - correct determination of the total size of rendered area, this fails often
 * - automate trying first with javascript and if this fails without js = { on, off, auto }
 * - text scoring
 * - figure out the Exception/thread structure
 */

public class SearchResultFinder {

	public static final int MINIMUM_NODECOUNT = 4;
	public static final int MINIMUM_RENDERED =  5;
	public static final double MIN_SIMILARITY_TH = 0.55;
	public static final double AVG_SIMILARITY_TH = 0.65;

	public static final boolean debug = false;

	final WebClient webClient;
	final HtmlElementGeometryServer renderServer;
	
	private Vector<CandidatePredicate> rejected = null;
	private Vector<CandidatePredicate> candidates = null;

	public SearchResultFinder() {
		webClient = new WebClient(BrowserVersion.FIREFOX_3_6);
		/* Sites for which JavaScript is needed: 
		 * marktplaats, otherwise cardinalities do not match
		 * 
		 */
		webClient.setJavaScriptEnabled(true);
		// webClient.setCssEnabled(true);
		// this.webClient = ScraperUtils.getWebClient(); // FAIL modified
		// WebClient, auto-delay, caching and no css

		renderServer = new HtmlElementGeometryServer();
	}
	
	public static Vector<String> robustSRF(String url) {
		SearchResultFinder srf = null;
		Vector<String> res = new Vector<String>();

		try {
			srf = new SearchResultFinder();

			Vector<CandidatePredicate> cp;

			srf.setJavaScriptEnabled(true);
			srf.ignoreAllErrors();
			cp = srf.analyzePage(url, new Vector<String>());
			if ((cp != null) && cp.size() > 0) {
				for (int i = 0; i < cp.size(); i++) {
					res.add(cp.elementAt(i).xpath());
				}
			}
			srf.close();
		} catch (Exception e) {
			if ( srf != null )
				srf.close();
		}
		return res;
	}
	
	
	int				errorCount = -1;
	StringBuilder	errorBuilder;
	
	public void ignoreAllErrors() {
		errorCount		= 0;
		errorBuilder	= new StringBuilder();
		final SearchResultFinder	srf_this = this;
		
	    // LogFactory.getFactory().setAttribute("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");

	    // java.util.logging.Logger.getLogger("com.gargoylesoftware.htmlunit").setLevel(Level.OFF); 
	    // java.util.logging.Logger.getLogger("org.apache.commons.httpclient").setLevel(Level.OFF);

	    webClient.setIncorrectnessListener(new IncorrectnessListener() {

	        public void notify(String arg0, Object arg1) {
	        	srf_this.ignoreError("incorrect-notify("+arg0+","+arg1+")");
	        }
	    });
	    
//	    webClient.setCssErrorHandler(new ErrorHandler() {
//
//	        @Override
//	        public void warning(CSSParseException exception) throws CSSException {
//	            // TODO Auto-generated method stub
//
//	        }
//
//	        @Override
//	        public void fatalError(CSSParseException exception) throws CSSException {
//	            // TODO Auto-generated method stub
//
//	        }
//
//	        @Override
//	        public void error(CSSParseException exception) throws CSSException {
//	            // TODO Auto-generated method stub
//
//	        }
//	    });
//	    
	    webClient.setJavaScriptErrorListener(new JavaScriptErrorListener() {

	        public void timeoutError(HtmlPage arg0, long arg1, long arg2) {
	        	srf_this.ignoreError("JsErr-timeout("+arg0+","+arg1+","+arg2+")");

	        }

	        public void scriptException(HtmlPage arg0, ScriptException arg1) {
	        	srf_this.ignoreError("JsErr-script("+arg0+","+arg1+")");
	        }

	        public void malformedScriptURL(HtmlPage arg0, String arg1, MalformedURLException arg2) {
	        	srf_this.ignoreError("JsErr-badURL("+arg0+","+arg1+","+arg2+")");
	        }

	        
	        public void loadScriptError(HtmlPage arg0, URL arg1, Exception arg2) {
	        	srf_this.ignoreError("JsErr-scriptLoad("+arg0+","+arg1+","+arg2+")");
	        }
	    });
	    
	    webClient.setHTMLParserListener(new HTMLParserListener() {

	        
	        public void warning(String arg0, URL arg1, int arg2, int arg3, String arg4) {
	        	srf_this.ignoreError("html-warning("+arg0+","+arg1+","+arg2+","+arg3+","+arg4+")");
	        }

	        
	        public void error(String arg0, URL arg1, int arg2, int arg3, String arg4) {
	        	srf_this.ignoreError("html-warning("+arg0+","+arg1+","+arg2+","+arg3+","+arg4+")");
	        }
	    });

	    webClient.setThrowExceptionOnFailingStatusCode(false);
	    webClient.setThrowExceptionOnScriptError(false);
	}
	
	public void ignoreError(String error) {
		if ((errorCount != -1) && (errorBuilder != null)) {
			errorCount++;
			errorBuilder.append(error);
			errorBuilder.append('\n');
		}
	}

	public boolean ignoredErrors() {
		if ( errorCount != -1 && errorCount > 0 )
			return true;
		else
			return false;
	}
	
	public String ignoredErrorList() {
		if ((errorCount != -1) && (errorBuilder != null)) {
			String res = errorBuilder.toString();
			
			errorCount = 0;
			errorBuilder.setLength(0);
			return res;
		}
		return "";
	}
	
	public void close() {
		if ( renderServer != null )
			renderServer.close();
		
	}
	
	public void setJavaScriptEnabled(boolean val) {
		webClient.setJavaScriptEnabled(val);
	}

	public Vector<CandidatePredicate> analyzePage(String search_page,
			Vector<String> terms) {
		HtmlPage page = null;
		if ( true )
			ignoreAllErrors(); // incomplete, better strategy here
		try {
			Hashtable<String, GeneralizedXPath> gxptab = new Hashtable<String, GeneralizedXPath>();

			page = webClient.getPage(search_page);
			// string2file(page.getDocumentElement().asXml(),"/tmp","google.xml");
			if (debug)
				System.out.println("Running SRF on:" + page.getTitleText());
			List<HtmlAnchor> la = (List<HtmlAnchor>) page.getAnchors();
			// System.out.println("Anchors: #="+l.size());
			for (int i = 0; i < la.size(); i++) {
				HtmlAnchor ha = la.get(i);
				String gxp_str = get_generalizedXPath(ha);
				GeneralizedXPath gxp = gxptab.get(gxp_str);
				if (gxp == null) {
					gxp = new GeneralizedXPath(page, gxp_str);
					gxptab.put(gxp_str, gxp);
				}
				gxp.addAnchor(ha);
			}
			this.rejected = new Vector<CandidatePredicate>();
			this.candidates = new Vector<CandidatePredicate>();
			Iterator<GeneralizedXPath> gxpit = gxptab.values().iterator();
			while (gxpit.hasNext()) {
				GeneralizedXPath gxp = gxpit.next();
				if (gxp.anchors.size() < MINIMUM_NODECOUNT) {
					if (debug)
						System.out.print("REJECT GXP:" + gxp);
				} else {
					if (debug)
						System.out.print("ACCEPT GXP:" + gxp);
					Vector<CandidatePredicate> cands = gxp
							.determineXPathCandidates();
					if (debug)
						System.out
								.println("After building pred table:\n" + gxp);
					for (int i = 0; i < cands.size(); i++)
						if (cands.elementAt(i).rejected())
							rejected.add(cands.elementAt(i));
						else
							candidates.add(cands.elementAt(i));
				}
			}
			for (int li = 0; li < candidates.size(); li++)
				for (int ri = li + 1; ri < candidates.size(); ri++) {
					CandidatePredicate l = candidates.elementAt(li);
					CandidatePredicate r = candidates.elementAt(ri);

					if (!l.rejected() && !r.rejected()) {
						if (l.resultnode.equals(r.resultnode)) {
							// results are the same reject the largest xpath
							if (l.xpathLength() < r.xpathLength())
								r.reject("other xpath selects same elements and is smaller");
							else
								l.reject("other xpath selects same elements and is smaller");
						}
					}
				}
			renderServer.open(search_page);
			for (int i = 0; i < candidates.size(); i++)
				if (!candidates.elementAt(i).rejected())
					score(candidates.elementAt(i), renderServer, terms);
			// renderServer.close();
			if (debug) {
				System.out.println("#!The final scores:");
				for (int i = 0; i < candidates.size(); i++)
					if (!candidates.elementAt(i).rejected())
						System.out.println(candidates.elementAt(i).explain());
			}
			// and finally sort on score
			Vector<CandidatePredicate> res = new Vector<CandidatePredicate>();
			for (int i = 0; i < candidates.size(); i++)
				if (!candidates.elementAt(i).rejected()) {
					int insertAt = 0;

					while (insertAt < res.size()
							&& candidates.elementAt(i).score() < res.elementAt(
									insertAt).score())
						insertAt++;
					res.insertElementAt(candidates.elementAt(i), insertAt);
				}
			return res;
		} catch (Exception e) {
			System.out.println("SearchResultFinder: " + e);
			e.printStackTrace();
		}
		return null;
	}
	
	public String explain_all() {
		int i;
		StringBuilder res = new StringBuilder();
		if ( candidates == null )
			return "NO SRF STARTED YET!";
		for(i=0; i<rejected.size(); i++) {
			res.append(rejected.elementAt(i).explain());
			res.append('\n');
		}
		for(i=0; i<candidates.size(); i++) {
			res.append(candidates.elementAt(i).explain());
			res.append('\n');
		}
		return res.toString();
	}

	private String get_generalizedXPath(HtmlAnchor anch) {
		StringBuilder res = new StringBuilder();

		for (Node n = anch.getParentNode(); !(n instanceof HtmlPage); n = n
				.getParentNode()) {
			HtmlElement el = (HtmlElement) n;
			// if ( el.getNodeName().equals("h3") ) System.out.println("************ FOUND H3 ***************");
			if (res.length() == 0)
				res.append(el.getNodeName());
			else {
				res.insert(0, '/');
				res.insert(0, el.getNodeName());
			}
		}
		res.insert(0, '/');
		return res.toString();
	}

	/*
	 * The score section
	 */

	public void score(CandidatePredicate cp,
			HtmlElementGeometryServer renderServer, Vector<String> terms) {
		if (debug)
			System.out.println("+ Scoring " + cp.card + " elements: "
					+ cp.xpath());
		if (debug)
			System.out.println("- ## TOP=" + renderServer.topElement());
		if (debug)
			System.out.println("- HtmlElement[0].asText() =\n"
					+ cp.resultnode.get(0).asText());

		handlePresentation(cp, renderServer, false);
		handleSimilarity(cp); // incomplete maybe this before presentation + rejected check
		handleTextScore(cp, terms);

		cp.computeFinalScore();
	}

	private void handlePresentation(CandidatePredicate cp,
			HtmlElementGeometryServer renderServer, boolean checkCardinality) {
		IntSet rows = new IntSet();
		IntSet cols = new IntSet();

		cp.totalArea = 0;
		Vector<HtmlElementGeometry> l = renderServer.getGeometry(cp.xpath());
		if ( checkCardinality && (cp.resultnode.size() != l.size()) ) {
			cp.reject("cardinality mismatch HtmlUnit and HtmlRenderServer: (HU-"+cp.resultnode.size() + " <> FF-"+ l.size()+")");
			return;
		}
		for (int i = 0; i < l.size(); i++) {
			rows.add(l.elementAt(i).x);
			cols.add(l.elementAt(i).y);
			cp.totalArea += l.elementAt(i).area();
		}
		cp.areaPerc = (int) ((100 * cp.totalArea) / renderServer.topElement()
				.area());
		if (debug)
			System.out.println("ROWS.size()=" + rows.size());
		if (debug)
			System.out.println("COLS.size()=" + cols.size());
		if (rows.size() == 1)
			// the elements are presented as a row
			cp.presentation = "row[" + cols.size() + "]";
		else if (cols.size() == 1)
			// the elements are presented as a column
			cp.presentation = "col[" + rows.size() + "]";
		else {
			int maxgrid = rows.size() * cols.size();
			int mingrid1 = (rows.size() - 1) * cols.size() + 1;
			int mingrid2 = rows.size() * (cols.size() - 1) + 1;
			if (l.size() <= maxgrid) {
				if (l.size() >= mingrid1 || l.size() >= mingrid2) 
					// the elements are presented as a grid
					cp.presentation = "grid[" + rows.size() + "," + cols.size()+ "]";
			}
		}
		if ( cp.presentation == null )
			cp.reject("predicate presents no row,column or grid");
		else if (cp.areaPerc < SearchResultFinder.MINIMUM_RENDERED)
			cp.reject("rendered area is too small: " + cp.areaPerc + "%["+cp.presentation+"]");
	}

	private void handleTextScore(CandidatePredicate cp, Vector<String> terms) {
		if (terms != null && (terms.size() == 0))
			return;
		// incomplete, do text scoring here
	}
	
	private void handleSimilarity(CandidatePredicate cp) {
		double sim[] = HtmlSimilarity.listSimilarity( cp.resultnode );
		cp.minSim = sim[0];
		cp.avgSim = sim[1];
		if (cp.minSim < MIN_SIMILARITY_TH)
			cp.reject("minimum similarity too low: " + cp.minSim);
		if (cp.avgSim < AVG_SIMILARITY_TH)
			cp.reject("average similarity too low: " + cp.avgSim);
		if (debug)
			System.out.println("#!final-score:" + cp.explain());
	}


	
}
