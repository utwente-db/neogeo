package nl.utwente.db.neogeo.db;

import java.io.InputStream;

public interface Database<RootModelObject extends Object> {
	public boolean openConnection();
	public void closeConnection();
	
	public boolean createDatabase(String databaseName);
	
	public void storeXML(String databaseName, String path, InputStream input);
	public void storeJSON(String json);
	
	public String getJSON(String query);
	public void streamXML(String xQuery);
	
	public void storeObject(RootModelObject object);
	public RootModelObject loadObject(RootModelObject templateObject);
}
