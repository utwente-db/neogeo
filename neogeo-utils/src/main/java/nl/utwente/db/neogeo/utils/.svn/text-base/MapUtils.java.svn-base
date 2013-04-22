package nl.utwente.db.neogeo.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapUtils {
	public static Map<? extends Object, ? extends Object> createMap(List<? extends Object> keys, List<? extends Object> values) {
		if (keys.size() != values.size()) {
			throw new RuntimeException("Sizes of lists don't match:\nkeys:" + keys + "\nvalues = " + values);
		}
		
		Map<Object, Object> result = new HashMap<Object, Object>();
		
		for (int i = 0; i < keys.size(); i++) {
			result.put(keys.get(i), values.get(i));
		}
		
		return result;
	}
}
