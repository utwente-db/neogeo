package nl.utwente.db.neogeo.twitter;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;

import org.json.simple.JSONValue;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class MyJSONRepository {
	
	private static boolean debug = false;

	@SuppressWarnings("rawtypes")
	private Map topJSONMap = null;

	@SuppressWarnings("rawtypes")
	public MyJSONRepository(Map topJSONMap) {
		this.topJSONMap = topJSONMap;
	}
	
	public Object getPath(String tag1) {
		Object res = topJSONMap.get(tag1);

		if (res != null)
			return res;
		return null;
	}

	@SuppressWarnings("rawtypes")
	public Object getPath(String tag1, String tag2) {
		Object res = topJSONMap.get(tag1);

		if (res != null) {
			if (res instanceof Map) {
				res = ((Map) res).get(tag2);
				if (res != null)
					return res;
			}
		}
		return null;
	}

	@SuppressWarnings("rawtypes")
	public Object getPath(String tag1, String tag2, String tag3) {
		Object res = topJSONMap.get(tag1);

		if (res != null) {
			if (res instanceof Map) {
				res = ((Map) res).get(tag2);
				if (res != null) {
					if (res instanceof Map) {
						res = ((Map) res).get(tag3);
						if (res != null)
							return res;
					}
				}
			}
		}
		return null;
	}
	
	public static final double obj2double(Object o) {
		if ( o instanceof Double )
			return ((Double)o).doubleValue();
		else if (o instanceof Long)
			return ((Long)o).doubleValue();
		else if (o instanceof Integer)
			return ((Integer)o).doubleValue();
		else
			throw new ClassCastException("cannot cast "+o+" to double");
		
	}
	
	public String polygon2linestring(Object lo) {	
		// System.out.println("CONVERTING-1: "+lo+", class="+lo.getClass().getName());
		// incomplete, check for incoming double, maybe check type
		try {
		if ( lo != null && lo instanceof LinkedList ) {
			@SuppressWarnings("rawtypes")
			List l = (List)((LinkedList)lo).get(0);
			
			if ( l.size() > 2 ) {
				// so it must be a proper bounding box
				StringBuffer res  = new StringBuffer();
				res.append("LINESTRING(");
				
				double dc1, dc2, df1, df2;
				dc1 = dc2= df1 = df2 = -1;
				for(int i=0; i<l.size(); i++) {
					@SuppressWarnings("rawtypes")
					List pi = (List)l.get(i);
					
					dc1 = obj2double(pi.get(0));
					dc2 = obj2double(pi.get(1));
					
					if ( i == 0 ) {
						df1 = dc1;
						df2 = dc2;
					} else 
						res.append(",");
					res.append(dc1);
					res.append(" ");
					res.append(dc2);
					if ( i == (l.size()-1)  ) {
						if ( !(dc1==df1 && dc2==df2) ) {
							// close the polygon
							res.append(",");
							res.append(df1);
							res.append(" ");
							res.append(df2);
						}
							
					}
				}
				res.append(")");
				return res.toString();
			}
		}} catch (Exception e) {
			if ( true ) {
				System.out.println("ERROR CONVERTING: "+lo.toString());
				System.out.println("Exception: "+e);
				e.printStackTrace();
			}
		}
		// throw new RuntimeException("UNEXPECTED EMPTY BBOX FOR PLACE");
		return null;
	}
	
	public String point2linestring(Object lo) {	
		try {
			List ll = (LinkedList)lo;
			double dp1 = obj2double(ll.get(0));
			double dp2 = obj2double(ll.get(1));
			StringBuffer res  = new StringBuffer();
			res.append("POINT(");
			res.append(dp1);
			res.append(" ");
			res.append(dp2);
			res.append(")");
			return res.toString();		
		} catch (Exception e) {
			if ( true ) {
				System.out.println("ERROR CONVERTING: "+lo.toString());
				System.out.println("Exception: "+e);
				e.printStackTrace();
			}
		}
		// throw new RuntimeException("UNEXPECTED EMPTY BBOX FOR PLACE");
		return null;
	}
	
	
	private static JSONParser parser = new JSONParser();
	
	private static ContainerFactory containerFactory = new ContainerFactory() {
		@SuppressWarnings("rawtypes")
		public List creatArrayContainer() {
			return new LinkedList();
		}

		@SuppressWarnings("rawtypes")
		public Map createObjectContainer() {
			return new LinkedHashMap();
		}
	};

	public static synchronized MyJSONRepository getRepository(
			String json_encoded) {
		try {
			@SuppressWarnings("rawtypes")
			Map json = (Map) parser.parse(json_encoded, containerFactory);
			if (debug) {
				System.out.println("JSON=" + json_encoded);
				@SuppressWarnings("rawtypes")
				Iterator iter = json.entrySet().iterator();
				System.out.println("==JSON iterate result==");
				while (iter.hasNext()) {
					@SuppressWarnings("rawtypes")
					Map.Entry entry = (Map.Entry) iter.next();
					System.out
							.println(entry.getKey() + "=>" + entry.getValue());
				}

				System.out.println("==toJSONString()==");
				System.out.println(JSONValue.toJSONString(json));
			}
			return new MyJSONRepository(json);
		} catch (ParseException pe) {
			System.out.println("Caught: " + pe);
			pe.printStackTrace(System.out);
		}
		return null;
	}
	
}
