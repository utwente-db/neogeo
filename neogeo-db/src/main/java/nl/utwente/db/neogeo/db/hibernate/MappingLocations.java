package nl.utwente.db.neogeo.db.hibernate;

import java.util.ArrayList;

public class MappingLocations extends ArrayList<String> {
	private static final long serialVersionUID = 1L;

	public MappingLocations() {
		System.out.println("te gek!");
		this.add("classpath*:/hibernate/model/*.hbm.xml");
	}
}
