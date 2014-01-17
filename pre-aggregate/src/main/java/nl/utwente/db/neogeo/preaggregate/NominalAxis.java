package nl.utwente.db.neogeo.preaggregate;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.logging.Logger;

public class NominalAxis extends AggregateAxis {
	
	protected static final Logger LOGGER = Logger.getLogger("nl.utwente.db.neogeo.preaggregate.NominalAxis");
	
	public static final int maxWords = 64;
	
	private String word_collection_column = null;
	private String wordlist_str = null;
	private String wordlist[]   = null;
	private int from;
	private int to;
	
	public NominalAxis(
			String	columnExpression, int from, int to) {
		super(columnExpression);
		init_from_to(from,to);
	}
	
	public NominalAxis(
			String word_collection_column, String word_index_column, String wordlist_str) {
		super(word_index_column);
		this.word_collection_column = word_collection_column;
		this.wordlist_str = wordlist_str;
		this.wordlist = tokenize_words(wordlist_str);
	
		init_from_to(0,wordlist.length);
	}

	private void init_from_to(int from, int to) {
		this.from = from;
		this.to = to;
	}
	
	public boolean wordlistNominal() {
		return wordlist_str != null;
	}
	
	public static final String WORDLISTNOMINAL = "WordlistNominal";
	
	public String fromFIELDstore() {
		if (wordlistNominal()) 
			return WORDLISTNOMINAL;
		else
			return ""+from;
	}
	
	public String toFIELDstore() {
		if (wordlistNominal())
			return wordlist_str;
		else
			return ""+to;
	}
	
	public static final String ALL = "ALL";
	
	public static String[] tokenize_words(String wordlist_str) {
		int nWords = 0;
		String res[] = new String[maxWords];
		StringTokenizer stok = new StringTokenizer(wordlist_str, ", \t");
 
		while (stok.hasMoreElements()) {
			String s = stok.nextElement().toString();
			if ( nWords >= maxWords )
				throw new RuntimeException("too much words in wordList: "+wordlist_str);
			res[nWords++] = s;
		}
		return Arrays.copyOfRange(res, 0, nWords);
	}
	
	public boolean isMetric() {
		return false;
	}
	
	public String type() {
		return "nominal";
	}
	
	public short N() {
		return (short)axisSize();
	}
		
	public int axisSize() {
		return to - from;
	}

	public int getIndex(Object value, boolean checkBounds) {
		return ((Integer)value).intValue() - from;
	}
	
	public Object reverseValue(int index) {
		return new Integer(index + from);
	}
	
	public boolean exactIndex(Object value) {
		return true;
	}
	
	public short maxLevels() {
		return (short)1;
	}
	
	public static final short log2(long base) {
		return (short) Math.ceil(Math.log(base) / Math.log((long)2));
	}
	
	public static final short pow2(int exp) {
		return (short) Math.round(Math.pow(2,exp));
	}
	
	
	public short bits() {
		/* The real axis size has to make room for 2 extra values for every index of a dimension:
		 * 0 - value is too small
		 * 2^n - 1 - value is too big
		 */
		int real_axis_size = axisSize() + 2;
		return log2(real_axis_size);
	}
	
	protected int tooLow() {
		return 0;
	}
	
	protected int tooHigh() {
		return pow2(bits()) - 1;
	}

	public int dimensionKeyValue(int d_i) {
		if ( d_i < 0 ) {
			if ( d_i == INDEX_TOO_SMALL )
				return tooLow();
			else if ( d_i == INDEX_TOO_LARGE )
				return tooHigh();
			else 
				throw new RuntimeException("bad dimension key value: "+d_i);
		} else
			return d_i + 1;
		
	}
	
	public String storageFormat(Object o) {
		return o.toString();
	}
	
	public String sqlType() {
		return "integer";	
	}
	
	public String sqlRangeFunction(Connection c, String fun) throws SQLException {
		return SqlUtils.gen_Create_Or_Replace_Function(
						c, fun, "v "+sqlType(), "integer",
						"", "\tRETURN v " + ((from != 0) ? (" - " + from) : "") + ";\n"

				);	
	}
	
	/**
	 * function does not take care of baseblocksize for the boundaries of the split
	 * and the delta/chunk
	 * @param n
	 * @return
	 */
	public Object[][] split(int n) {
		throw new RuntimeException("INCOMPLETE");
	}
	
	public String toString() {
		return "NominalAxis(colExpr="+columnExpression()+", from="+from+", to="+to+")";
	}

	public AxisSplitDimension splitAxis(Object low, Object high, int cnt) {
		throw new RuntimeException("INCOMPLETE");
	}
	
	int getWordIndex(String word) {
		if ( wordlist != null ) {
			for (int i=0; i<wordlist.length; i++) {
				if ( word.equals(wordlist[i]))
					return i;
			}
		}
		return -1;
	}
	
	/*
	 * Preprocessing stuff
	 * 
	 */
	public void tagWordIds2Table(Connection c, String schema, String org_table, String new_table) throws SQLException {
		if ( !wordlistNominal() )
			throw new SQLException("cannot tag words in plain numeric NominalAxis");
		org_table = schema + "." + org_table;
		new_table = schema + "." + new_table;
		String wl_table = schema + "." + "wordlist";

		StringBuffer sql = new StringBuffer();
		sql.append("DROP TABLE IF EXISTS "+new_table+";\n");
		sql.append("DROP TABLE IF EXISTS "+wl_table+";\n");
		sql.append("CREATE TABLE "+wl_table+" (word text,wordid int);\n");
		for(int i=0; i<wordlist.length; i++) {
			String match_str = wordlist[i];
			
			if ( match_str.equals(ALL))
				match_str = "";
			sql.append("INSERT INTO "+wl_table+" (word,wordid) VALUES(\'" + match_str.toLowerCase() + "\',"+i+");\n");
		}
		sql.append("\nSELECT "+org_table+".*, "+wl_table+".wordid "+"AS "+columnExpression()+"\nINTO "+new_table+"\nFROM "+org_table+", "+wl_table +
					"\nWHERE strpos(lower("+org_table+"."+word_collection_column+"),"+wl_table+"."+"word)>0;\n");
		sql.append("\nDROP TABLE IF EXISTS "+wl_table+";\n");
		System.out.println(sql);
		SqlUtils.executeSCRIPT(c, sql.toString());
	}

}
