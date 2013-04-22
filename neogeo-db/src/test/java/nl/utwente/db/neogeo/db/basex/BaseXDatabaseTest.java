package nl.utwente.db.neogeo.db.basex;

import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;

import nl.utwente.db.neogeo.utils.StringUtils;

import org.junit.Ignore;
import org.junit.Test;

@Ignore // BaseXDatabase is yet to be implemented and tested, introduce connection pooling etc.
public class BaseXDatabaseTest {
	
	@Test
	public void createDatabase() {
		System.out.println("======== " + this.getClass().getSimpleName() + ".createDatabase()" + " ========");
		BaseXDatabase database = getBaseXTestDatabase();
		
		System.out.println(database);
		
		try {
			database.openConnection();
			database.createDatabase("facebook");

		} catch (Exception e) {
			assertTrue(true);
			throw new RuntimeException("Test failed: " + e.getMessage(), e);
		} finally {
			database.closeConnection();
		}
	}
	
	@Test
	public void storeXML() {
		System.out.println("======== " + this.getClass().getSimpleName() + ".storeXML()" + " ========");
		BaseXDatabase database = getBaseXTestDatabase();
		
		try {
			database.openConnection();
			
			String xml = "<aap><beer/></aap>";
			InputStream inputStream = StringUtils.stringToInputStream(xml);
	
			database.storeXML("facebook", "//users/facebook", inputStream);
		} finally {
			database.closeConnection();
		}
	}

	public void storeJSON(String json) {
		// TODO Auto-generated method stub

	}

	@Test
	public void streamXML() throws InterruptedException {
		System.out.println("======== " + this.getClass().getSimpleName() + ".streamXML()" + " ========");
		BaseXDatabase database = getBaseXTestDatabase();
		
		try {
			database.openConnection();
			
			for (int i = 0; i < 10000; i++) {
				database.streamXML("doc(\"/home/victor/tmp/factbook.xml\")//aap/beer/clown");
			}

			// Without this println this test closes System.out too fast and cannot display all results
			System.out.println();
		} finally {
			database.closeConnection();
		}
	}
	
	public void storeObject(Object object) {
		// TODO Auto-generated method stub
		
	}

	public Object loadObject(Object templateObject) {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean openConnection() {
		// TODO Auto-generated method stub
		return false;
	}

	public void closeConnection() {
		// TODO Auto-generated method stub
		
	}

	public void streamJSON(String query, OutputStream out) {
		// TODO Auto-generated method stub
		
	}

	public static BaseXDatabase getBaseXTestDatabase() {
		return new BaseXDatabase("localhost", 1984, "admin", "admin", System.out);
	}
}
