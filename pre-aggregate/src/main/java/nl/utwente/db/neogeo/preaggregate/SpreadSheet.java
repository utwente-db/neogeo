package nl.utwente.db.neogeo.preaggregate;

import java.io.FileWriter;
import java.io.IOException;

public class SpreadSheet {

	private String			descriptor[][];
	private StringBuffer	buffer;
	
	SpreadSheet() {
	}
	
	public void start(String[][] descriptor) {
		this.descriptor = descriptor;
		buffer = new StringBuffer();

		buffer.append("<!DOCTYPE html>\n");
		buffer.append("<html>\n");
		buffer.append("<body>\n");

		buffer.append("<table border=\"1\">\n");
		buffer.append("<tr>\n");
		for (int i = 0; i < descriptor.length; i++) {
			buffer.append("<th>" + descriptor[i][0] + "</th>\n");
		}
		buffer.append("</tr>\n");
	}

	public void add(String[] values) {
//		if ( values.length != descriptor.length )
//			throw new RuntimeException("BAD# parameters");
		buffer.append("<tr>\n");
		for(int i=0; i<values.length; i++) {
			buffer.append("<td "+descriptor[i][1]+">\n");
			buffer.append(values[i]);
			buffer.append('\n');
			buffer.append("</td>\n");
		}
		buffer.append("</tr>\n");
	}
	
	public void add(String s1, String s2, String s3) {
		String sv[] = {s1,s2,s3};
		add(sv);
	}
	
	public void add(String s1,String s2,String s3,String s4,String s5) {
		String sv[] = {s1,s2,s3,s4,s5};
		add(sv);
	}
	
	public void add(String s1,String s2,String s3,String s4,String s5,String s6, String s7, String s8) {
		String sv[] = {s1,s2,s3,s4,s5,s6,s7,s8};
		add(sv);
	}
	
	public void add(String s1,String s2,String s3,String s4,String s5,String s6, String s7, String s8, String s9, String s10) {
		String sv[] = {s1,s2,s3,s4,s5,s6,s7,s8,s9,s10};
		add(sv);
	}
	
	public void add(String s1,String s2,String s3,String s4,String s5,String s6, String s7, String s8, String s9, String s10, String s11) {
		String sv[] = {s1,s2,s3,s4,s5,s6,s7,s8,s9,s10,s11};
		add(sv);
	}
	
	public String finish(String dir, String file) {
		buffer.append("</table>\n");
		buffer.append("</body>\n");
		buffer.append("</html>\n");
		String res = buffer.toString();
		if ( dir != null && file != null) {
			try {
		        FileWriter out = new FileWriter(dir+"/"+file);
		        out.write(res,0,res.length());
		        out.close();
		    }
		    catch (IOException e) {
		      System.out.println("#!SpreadSheet:finish(): Caught: "+e);
		    }   
		}
		return res;

	}
	
	static final String measurements[][] = {
		{"What",""},
		{"#Tweets","align=\"right\""},
		{"N",""},
		{"#cells","align=\"right\""},
		{"#i++cells","align=\"right\""},
		{"Query",""},
		{"result","align=\"right\""},
		{"#unopt(ms)","align=\"right\""},
		{"#i++opt(ms)","align=\"right\""},
		{"#corr(ms)","align=\"right\""},
		{"#direct-pg(ms)","align=\"right\""}
	};
	
	public static void main(String[] argv) {
		SpreadSheet ss = new SpreadSheet();
		
		ss.start(measurements);
		ss.add("london_hav","7.881","4","2.204","5.644");
		ss.add("","","","","","q1/count","7","45","4","","8");
		ss.add("","","","","","q1/sum","493","45","4","","8");
		ss.add("","","","","","q2/count","2860","35","5","","15");
		ss.add("","","","","","q2/sum","189.583","36","4","","17");
		ss.add("london","371.460","4","60.264","93.414");
		ss.add("","","","","","q3/count","36.694","48","5","","204");
		ss.add("","","","","","q3/sum","2587042","48","5","","230");
		ss.add("","","","","","q4/count","310.443","150","10","","765");
		ss.add("","","","","","q4/sum","22.399.518","155","10","","985");
		ss.add("uk","7.625.502","4","1.209.540","2.761.187");
		ss.add("","","","","","q5/count","10.548","-","5","310","157");
		ss.add("","","","","","q5/sum","679.862","-","5","","166");
		ss.add("","","","","","q6/count","1.033.632,","-","4","123","19.6");
		ss.add("","","","","","q7/count","7.625.502","-","17","-","17.161");
		ss.add("","","","","","q7/sum","522.818.962","-","17","","21.334");
		ss.add("uk-btree(l,x,y)><","7.625.502","4","1.209.540","0");
		ss.add("","","","","","q5/count","10.548","-","33","","157");
		ss.add("","","","","","q6/count","7.625.502","-","215","","17.161");

		System.out.println(ss.finish("/Users/flokstra","results.html"));
	}

}
