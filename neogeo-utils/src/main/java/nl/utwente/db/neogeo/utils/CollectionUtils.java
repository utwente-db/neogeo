package nl.utwente.db.neogeo.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CollectionUtils {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static List asList(Iterable iterable) {
		List result = new ArrayList();
		
		for (Object item : iterable) {
			result.add(item);
		}
		
		return result;
	}

	public static Class<?> getTypeOfFirstElement(Collection<?> collection) {
		if (collection.isEmpty()) {
			return null;
		} else {
			return getFirstObject(collection).getClass();
		}
	}
	
	public static Object getFirstObject(Collection<?> collection) {
		Iterator<?> iterator = collection.iterator();
		
		if (iterator.hasNext()) {
			return iterator.next();
		} else {
			return null;
		}
	}
}
