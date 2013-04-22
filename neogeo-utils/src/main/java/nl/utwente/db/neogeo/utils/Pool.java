package nl.utwente.db.neogeo.utils;

import java.util.List;

public interface Pool<Type> extends List<Type> {
	public Type getRandomItem();
}
