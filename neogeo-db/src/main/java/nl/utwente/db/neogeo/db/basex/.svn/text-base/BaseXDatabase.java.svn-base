package nl.utwente.db.neogeo.db.basex;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.db.Database;

import org.basex.server.ClientSession;

// TODO is it possible to use inputstreams and outputstreams directly?
/**
 * This class is not thread-safe, due to ClientSession not being thread-safe. Opening up a new socket every time would be too expensive though.
 */
public class BaseXDatabase implements Database<Object> {
	protected ClientSession session;
	protected String host;
	protected int port;
	protected String username;
	protected String password;
	protected OutputStream out;
	
	public ClientSession getSession() {
		return session;
	}

	public void setSession(ClientSession session) {
		this.session = session;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @deprecated This may not match with the ClientSession's output stream.
	 * For convenience (e.g. for using the Spring framework) the getters and setters are still available.
	 */
	@Deprecated
	public OutputStream getOut() {
		return out;
	}

	public void setOut(OutputStream out) {
		this.out = out;
	}

	/**
	 */
	public BaseXDatabase(String host, int port, String username, String password, OutputStream out) {
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.out = out;
	}
	
	public boolean createDatabase(String databaseName) {
		if (session == null) {
			this.openConnection();
		}
		
		try {
			// TODO put this in a configurable location
			session.execute("create database " + databaseName);
			return true;
		} catch (IOException e) {
			return false;
		}
	}
	
	public boolean openConnection() {
		try {
			session = new ClientSession(host, port, username, password);
			session.setOutputStream(out);

			return true;
		} catch (IOException e) {
			throw new NeoGeoException("Could not connect to BaseX database.", e);
		}
	}
	
	public void storeXML(String databaseName, String path, InputStream input) {
		if (session == null) {
			this.openConnection();
		}
		
		try {
			session.execute("open " + databaseName);
			session.add(path, input);
		} catch (IOException e) {
			throw new NeoGeoException("Could not store in BaseX database: " + path, e);
		}
	}

	public void storeJSON(String json) {
		// TODO Auto-generated method stub

	}

	public void streamXML(String xQuery) {
		if (session == null) {
			this.openConnection();
		}
		
		try {
			session.query(xQuery).execute();
		} catch (IOException e) {
			throw new NeoGeoException("Could not read from BaseX database: " + xQuery, e);
		}
	}
	
	
	public void storeObject(Object object) {
		// TODO Auto-generated method stub
		
	}

	public Object loadObject(Object templateObject) {
		// TODO Auto-generated method stub
		return null;
	}

	public void closeConnection() {
		try {
			session.close();
		} catch (IOException e) {
			// TODO log this instead
			throw new NeoGeoException("Unable to close connection to BaseX", e);
		}
	}

	public String getJSON(String query) {
		// TODO Auto-generated method stub
		return null;
	}
}
