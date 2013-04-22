package nl.utwente.db.neogeo.scraper.srf;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import com.gargoylesoftware.htmlunit.html.HtmlElement;

public class HtmlSimilarity {
	
	private static class StringTab {
		int count;
		Hashtable<String, Integer> table;

		StringTab() {
			count = 0;
			table = new Hashtable<String, Integer>();
		}

		public int size() {
			return count;
		}

		public int getIndex(String s) {
			Integer res = table.get(s);

			if (res == null) {
				res = new Integer(count++);
				table.put(s, res);
			}
			return res.intValue();
		}
	}
	
	public double nodeSimilarity(HtmlElement le, HtmlElement re) {	
		// return cosine similarity between two HtmlElement Nodes
		int vsize = 4;
		StringTab st = new StringTab();

		int lv[] = getSimVector(le, "", new int[vsize], st );
		vsize = lv.length;
		int rv[] = getSimVector(re, "", new int[vsize], st );
		vsize = rv.length;
		double simval = sim(lv, rv, vsize);
		
		return simval;
	}

	public static double[] listSimilarity(List<HtmlElement> l) {	
		// return cosine similarity of a List of HtmlElement Nodes
		int i, j;
		int vsize = 4;
		StringTab st = new StringTab();
		Vector<int[]> v_all = new Vector<int[]>();

		for (i = 0; i < l.size(); i++) {
			int[] simvec = getSimVector(l.get(i), "",
					new int[vsize], st);
			v_all.add(simvec);
			vsize = simvec.length;
			// System.out.println("#!SIMVEC="+Arrays.toString(simvec));
		}
		// first compute sum of all vectors
		int[] vsum = new int[vsize];
		Arrays.fill(vsum, 0); // incomplete obsolete
		for (i = 0; i < v_all.size(); i++) {
			int[] vi = v_all.elementAt(i);
			for (j = 0; j < vi.length; j++)
				vsum[j] += vi[j];
		}
		// now take the average
		for (j = 0; j < vsum.length; j++)
			vsum[j] = vsum[j] / v_all.size();
		// if (debug)
		// System.out.println("- avg-sim-vector="+Arrays.toString(vsum));
		// now compute the similarity between every vector and the average
		double minSim = 1;
		double sumSim = 0;
		for (i = 0; i < v_all.size(); i++) {
			double simval = sim(v_all.elementAt(i), vsum, st.size());
			// if (debug)
			// System.out.println("- sum-vector-dist"+Arrays.toString(v_all.elementAt(i))+"="+simval);
			sumSim += simval;
			if (simval < minSim)
				minSim = simval;
		}
		double res[] = {minSim, sumSim / v_all.size()};
		
		return res;
	}
	
	
	private static int[] getSimVector(HtmlElement el, String curpath, int simvec[],
			StringTab st) {
		String newpath = curpath + "/" + el.getNodeName();
		int vindex = st.getIndex(newpath);
		if (vindex >= simvec.length)
			simvec = Arrays.copyOf(simvec, 2 * simvec.length);
		simvec[vindex]++;
		Iterator<HtmlElement> it = el.getChildElements().iterator();
		while (it.hasNext()) {
			HtmlElement ch = it.next();
			simvec = getSimVector(ch, newpath, simvec, st);
		}
		return simvec;
	}

	private static double sim(int[] vs, int[] vl, int last) {
		if (vs.length > vl.length) {
			int tmp[] = vs;
			vs = vl; // vs must always be the smallest
			vl = tmp;
		}
		double dp_sum = 0;
		for (int i=0; (i < vs.length && i < last); i++)
			dp_sum += (vs[i] * vl[i]);
		return dp_sum / (vlength(vs) * vlength(vl));
	}

	private static double vlength(int v[]) {
		int sum = 0;

		for (int i = 0; i < v.length; i++)
			sum += (v[i] * v[i]);
		return Math.sqrt(sum);

	}

}
