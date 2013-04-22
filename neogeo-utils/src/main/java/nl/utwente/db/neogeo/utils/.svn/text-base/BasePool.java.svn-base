package nl.utwente.db.neogeo.utils;

import java.util.ArrayList;

public class BasePool<Type extends Object> extends ArrayList<Type> implements Pool<Type> {
	private static final long serialVersionUID = 1L;

	public Type getRandomItem() {
		double randomNumber = Math.random();
		int index = (int)Math.floor(randomNumber * size());
		
		return get(index);
	}
}
