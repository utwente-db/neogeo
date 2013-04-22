package nl.utwente.db.neogeo.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import nl.utwente.db.neogeo.core.NeoGeoException;

public class NeoGeoProperties extends Properties {
	private static final long serialVersionUID = 1L;
	private static NeoGeoProperties instance;
	
	private String fileName = "conf/neogeo.properties";
	private boolean initialized = false;
	
	static {
		instance = new NeoGeoProperties();
	}
	
	private NeoGeoProperties() {
		super();
	}
	
	private NeoGeoProperties(String fileName) {
		super();
		this.setFileName(fileName);
	}
	
	private void init() {
		if (isInitialized()) {
			return;
		}
		
		try {
			this.load(new FileInputStream(FileUtils.getFileFromClassPath(fileName)));
			
			initialized = true;
		} catch (FileNotFoundException e) {
			throw new NeoGeoException("Unable to initialize NeoGeoProperties", e);
		} catch (IOException e) {
			throw new NeoGeoException("Unable to initialize NeoGeoProperties", e);
		}
	}
	
	public static NeoGeoProperties getInstance() {
		if (!instance.isInitialized()) {
			instance.init();
		}
		
		return instance;
	}
	
	public static void setInstance(NeoGeoProperties instance) {
		NeoGeoProperties.instance = instance;
	}

	public boolean isInitialized() {
		return initialized;
	}

	public void setInitialized(boolean initialized) {
		this.initialized = initialized;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
