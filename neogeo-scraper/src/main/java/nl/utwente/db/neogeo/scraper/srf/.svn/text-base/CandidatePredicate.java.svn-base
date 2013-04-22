package nl.utwente.db.neogeo.scraper.srf;

import java.text.DecimalFormat;
import java.util.BitSet;
import java.util.List;

import com.gargoylesoftware.htmlunit.html.*;

public class CandidatePredicate {
	public GeneralizedXPath gxp;
	public int level;
	public String predicate;
	public BitSet resultset;
	public List<HtmlElement> resultnode;
	public int card;
	private String reject_reason;
	public int start_elm_level;
	public int elm_level;
	public int dos_level;

	// the scoring section
	private double score;
	public String presentation;
	public int totalArea;
	public int areaPerc;
	public double avgSim;
	public double minSim;
	public double textscore;
	public double xtra_points;

	CandidatePredicate(GeneralizedXPath gxp, int level, String predicate) {
		this.gxp = gxp;
		this.level = level;
		this.predicate = predicate;
		this.resultset = null;
		this.card = -1;
		this.reject_reason = null;
		this.start_elm_level = -1;
		this.elm_level = -1;
		this.dos_level = -1;
		this.resultnode = null;
		//
		this.score = -1;
		this.presentation = null;
		this.totalArea = 0;
		this.areaPerc = 0;
		this.avgSim = 0;
		this.minSim = 0;
		this.textscore = 1;
		this.xtra_points = 0;
	}

	public void computeFinalScore() {
		if (rejected())
			score = 0;
		else {
			// score = ((areaPerc/100) + avgSim + textscore)/3;
			score = (((double) areaPerc / 100.0) + avgSim + textscore) / 3.0;
		}
	}

	public String explain() {
		StringBuilder res = new StringBuilder();
		
		res.append( "+ "+xpath()+" = " );
		if ( rejected() )
			res.append("rejected: "+reject_reason);
		else {
			res.append("{");
			res.append("score=" + new DecimalFormat("0.000").format(score));
			res.append(", card=" + this.card);
			if ( presentation != null )
				res.append(", "+presentation);
			res.append(", area=" + areaPerc + "%");
			res.append(", minS=" + new DecimalFormat("0.00").format(minSim));
			res.append(", avgS=" + new DecimalFormat("0.00").format(avgSim));
			res.append("}");
			
		}
		return res.toString();
	}

	public void reject(String reason) {
		this.reject_reason = reason;
	}

	public boolean rejected() {
		return reject_reason != null;
	}

	public boolean hasPredicate() {
		return predicate != null;
	}

	public int predicateLength() {
		if (hasPredicate())
			return predicate.length();
		else
			return 0;
	}

	public int xpathLength() {
		return xpath().length();
	}

	public String basicXPath() {
		StringBuilder res = new StringBuilder();

		for (int i = 0; i < gxp.steps.size(); i++) {
			res.append("/" + gxp.steps.elementAt(i));
			if (i == level)
				res.append("[" + predicate + "]");
		}
		res.append("/a");
		return res.toString();
	}

	public String getModifiedXPath(int elmlevel, int doslevel) {
		StringBuilder res = new StringBuilder();
		boolean inpredicate = false;

		if (doslevel > -1)
			res.append('/');
		for (int i = 0; i < gxp.steps.size(); i++) {
			if (doslevel <= i && (!hasPredicate() || doslevel <= level))
				res.append("/" + gxp.steps.elementAt(i));
			if (i == level)
				res.append("[" + predicate + "]");
			if (i == elmlevel) {
				res.append("[.");
				inpredicate = true;
			}
		}
		res.append("/a");
		if (inpredicate)
			res.append(']');
		return res.toString();
	}

	public String xpath() {
		return getModifiedXPath(elm_level, dos_level);
	}

	public double score() {
		return score;
	}

	public CandidatePredicate cloneSmallerResult(int elm_level,
			List<HtmlElement> resultnode) {
		CandidatePredicate res = null;

		res = new CandidatePredicate(gxp,level,predicate);
		res.start_elm_level = elm_level - 1;
		res.elm_level = elm_level;
		res.card = resultnode.size();
		res.resultset = new BitSet(); // incomplete, design error
		res.resultnode = resultnode;
		return res;
	}

	public String toString() {
		StringBuilder res = new StringBuilder();

		final String prefix = "    ! ";
		if (hasPredicate())
			res.append("" + level + " " + gxp.steps.elementAt(level) + "["
					+ predicate + "]\n");
		else
			res.append("x empty predicate\n");
		res.append(prefix + "basic xpath=" + basicXPath() + "\n");
		if (rejected())
			res.append(prefix + "REJECTED: " + reject_reason + "\n");
		if (resultset != null)
			res.append(prefix + "result set: " + resultset + " C="
					+ resultset.cardinality() + "\n");
		if (elm_level > -1)
			res.append(prefix + "elm_level=" + elm_level + ", xpath="
					+ getModifiedXPath(elm_level, -1) + "\n");
		if (dos_level > -1)
			res.append(prefix + "dos_level=" + dos_level + ", xpath="
					+ getModifiedXPath(elm_level, dos_level) + "\n");
		return res.toString();
	}

}