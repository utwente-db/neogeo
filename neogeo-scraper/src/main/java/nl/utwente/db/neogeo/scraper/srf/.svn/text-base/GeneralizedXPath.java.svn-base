package nl.utwente.db.neogeo.scraper.srf;

import java.util.Iterator;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;
import java.util.BitSet;

import org.w3c.dom.Node;

import com.gargoylesoftware.htmlunit.html.*;

public class GeneralizedXPath {

	public HtmlPage page;
	public String path;
	Vector<HtmlAnchor> anchors;

	Vector<String> steps;
	Vector<CandidatePredicate> preds;

	public final boolean text_search = true; // incomplete
	public final int MINIMUM_TEXT = 6;

	public GeneralizedXPath(HtmlPage page, String path) {
		this.page = page;
		this.path = path;
		this.anchors = new Vector<HtmlAnchor>();
		this.steps = null;
		this.preds = null;
	}

	int topLevel() {
		return steps.size() - 1;
	}

	public void addAnchor(HtmlAnchor a) {
		anchors.add(a);
	}

	@SuppressWarnings("unchecked")
	public Vector<CandidatePredicate> determineXPathCandidates() {
		int i;

		// implement section 3.1.3 of the paper
		steps = new Vector<String>();
		preds = new Vector<CandidatePredicate>();
		// create predicate table as a Vector of steps
		for (Node n = anchors.elementAt(0).getParentNode(); !(n instanceof HtmlPage); n = n
				.getParentNode()) {
			HtmlElement el = (HtmlElement) n;
			steps.add(0, el.getNodeName());
		}
		// extract all predicates (==attributes) from all anchors to root
		preds.add(new CandidatePredicate(this, -1, null)); // the no-predicate
															// predicate
		Hashtable<HtmlElement, HtmlElement> done = new Hashtable<HtmlElement, HtmlElement>();
		for (i = 0; i < anchors.size(); i++) {
			extractPredicates(anchors.elementAt(i), steps.size() - 1, done);
		}

		// compute the bitmap of the anchors which are selected by basic xpath
		for (i = 0; i < preds.size(); i++) {
			CandidatePredicate cp = preds.elementAt(i);

			List<HtmlElement> l = (List<HtmlElement>) page.getByXPath(cp
					.basicXPath());

			cp.resultset = matchListWithAnchors(l);
			cp.resultnode = l;
			cp.card = cp.resultset.cardinality();
		}
		// reject (for now) redundant predicates
		for (i = 0; i < preds.size(); i++) {
			CandidatePredicate cp = preds.elementAt(i);

			if (cp.resultset.cardinality() < SearchResultFinder.MINIMUM_NODECOUNT) {
				cp.reject("not enough anchors are selected ("
						+ cp.resultset.cardinality() + ")");
			} else if (cp.hasPredicate()
					&& (cp.resultset.cardinality() == anchors.size())) {
				cp.reject("predicate does not restrict");
			} else {
				for (int li = 0; li < preds.size(); li++)
					for (int ri = li + 1; ri < preds.size(); ri++) {
						CandidatePredicate l = preds.elementAt(li);
						CandidatePredicate r = preds.elementAt(ri);

						if (!l.rejected() && !r.rejected()) {
							// check if both valid preds generate same result
							if (l.resultset.equals(r.resultset)) {
								// reject the largest predicate
								if (l.predicateLength() < r.predicateLength())
									r.reject("other predicate selects same and is smaller");
								else
									l.reject("other predicate selects same and is smaller");
							}
						}
					}
			}
		}
		// compute the highest level node in the path with the same number of
		// results
		for (i = 0; i < preds.size(); i++) {
			CandidatePredicate cp = preds.elementAt(i);
			if (!cp.rejected()) {
				if (cp.start_elm_level == -1)
					cp.start_elm_level = topLevel() + 1;
				// else
				// cp is cloned with an advanced elm_level
				for (int elmlevel = cp.start_elm_level; elmlevel >= 0; --elmlevel) {
					String trypath = cp.getModifiedXPath(elmlevel, -1);
					// System.out.println("#!ELM-TRY["+elmlevel+"]:"+trypath);
					List<HtmlElement> l = (List<HtmlElement>) page
							.getByXPath(trypath);
					if (l.size() == cp.card) {
						cp.elm_level = elmlevel;
						cp.resultnode = l; // store last best try
					} else {
						if (l.size() >= SearchResultFinder.MINIMUM_NODECOUNT) {
							/*
							 * the result has shrunk but still more than the
							 * MINIMUM_NODES count We create a copy of the
							 * predicate starting at this smaller level
							 */
							CandidatePredicate new_cp = cp.cloneSmallerResult(elmlevel,l);
							preds.add( new_cp );
						}
						break; // stop 'upping' this element
					}
				}
				int maxlevel;

				if (cp.hasPredicate())
					maxlevel = cp.level;
				else
					maxlevel = cp.elm_level;
				for (int doslevel = 0; (doslevel <= maxlevel)
						&& (doslevel <= cp.elm_level); ++doslevel) {
					String trypath = cp
							.getModifiedXPath(cp.elm_level, doslevel);
					List<HtmlElement> l = (List<HtmlElement>) page
							.getByXPath(trypath);
					// System.out.println("#!DOS-TRY[dos="+doslevel+",elm="+cp.elm_level+"]:"+trypath+"="+l.size()
					// + " el(C="+cp.card+")");

					if (l.size() == cp.card) {
						cp.dos_level = doslevel;
						cp.resultnode = l; // store last best try
					} else
						break;
				}
			}
			// remove all elements which display not enough text
			if (text_search && !cp.rejected() && cp.resultnode.size() > 0) {
				String astext = cp.resultnode.get(0).asText();

//				if (astext.isEmpty())
//					cp.reject("no visible text on screen");
//				else if (astext.length() < MINIMUM_TEXT)
//					cp.reject("visible text too small");
//				else if (astext.matches("\\s*"))
//					cp.reject("visible text contains only whitespaces");
			}
		}
		return preds;
	}

	private BitSet matchListWithAnchors(List<HtmlElement> l) {
		BitSet res = new BitSet(anchors.size());

		for (int i = 0; i < anchors.size(); i++)
			res.set(i, (l.indexOf(anchors.elementAt(i)) >= 0));
		return res;
	}

	private void extractPredicates(HtmlAnchor an, int level,
			Hashtable<HtmlElement, HtmlElement> done) {
		HtmlElement el = (HtmlElement) an;
		while (level >= 0) {
			el = (HtmlElement) el.getParentNode();
			if (done.containsKey(el)) {
				// System.out.println("SKIPPING!!!");
				return; // visited this element allready
			}
			// System.out.println("XI:"+level+" "+steps.elementAt(level).name+
			// " "+el.getNodeName());
			// incomplete, now extract the attributes
			Iterator<DomAttr> ai = el.getAttributesMap().values().iterator();
			while (ai.hasNext()) {
				DomAttr da = ai.next();
				addCandidatePredicates(level, da.getName(), da.getValue());
			}
			level--;
			done.put(el, el);
		}

	}

	private void addCandidatePredicates(int level, String name, String value) {
		// System.out.println("ATTR:"+name+"="+value);
		if ( value.indexOf('\'')  != -1 )
			return; // skip these shitty attr values for time beiing
		if (name.equals("class")) {
			String[] single_classes = value.split("[ \t]");
			if (single_classes.length > 1) {
				for (int i = 0; i < single_classes.length; i++) {
					addCandidatePredicate(level, "contains(@class,\'"
							+ single_classes[i] + "\')");
				}
				return;
			}
		} else if (name.equals("id")) {
			int last = value.length() - 1;

			if ( (value.length() > 0) && Character.isDigit(value.charAt(last))) {
				--last;
				while (last >= 0 && Character.isDigit(value.charAt(last)))
					--last;
				if (last >= 0) { // forget just numerical ids?
					addCandidatePredicate(level,
							"starts-with(@id,\'" + value.substring(0, last + 1)
									+ "\')");
				}
				return;
			}

		}
		addCandidatePredicate(level, "@" + name + "=\'" + value + "\'");
	}

	private void addCandidatePredicate(int level, String str_predicate) {
		for (int i = 0; i < preds.size(); i++) {
			CandidatePredicate cp = preds.elementAt(i);

			if (cp.level == level && cp.predicate.equals(str_predicate))
				return;
		}
		preds.add(new CandidatePredicate(this, level, str_predicate));
	}

	public String toString() {
		StringBuffer res = new StringBuffer();

		res.append("* Generalized XPath: " + path + " has " + anchors.size()
				+ " Anchors\n");
		if (steps != null) {
			int i;

			res.append("+ levels: ");
			for (i = 0; i < steps.size(); i++) {
				res.append("(" + i + "=" + steps.elementAt(i) + ")");
			}
			res.append("\n");
			res.append("+ Candidate predicates:\n");
			for (i = 0; i < preds.size(); i++) {
				CandidatePredicate cp = preds.elementAt(i);

				res.append("- " + cp + "\n");
			}
		}
		return res.toString();
	}

}
