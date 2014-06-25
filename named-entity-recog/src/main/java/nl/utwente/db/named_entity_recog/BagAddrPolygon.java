package nl.utwente.db.named_entity_recog;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class BagAddrPolygon extends DefaultHandler {
	
	Connection c =  null;
	
	public BagAddrPolygon() throws SQLException {
		c = null;
	}
	
	public BagAddrPolygon(Connection c) throws SQLException {
		// this.entityDB = new GeoNameEntityTable( GeoNamesDB.geoNameDBConnection() );
	}
	
	private static String convertToFileURL(String filename) {
        String path = new File(filename).getAbsolutePath();
        if (File.separatorChar != '/') {
            path = path.replace(File.separatorChar, '/');
        }

        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return "file:" + path;
    }
	
	public  void test() throws IOException, SQLException {
		// readRestJson("/Users/flokstra/tomtom/Restaurants50/vrt-rest-0000_0050.json");
		System.out.println("BAP started");
	}
	
	public static void test_parser() {
		try {
		SAXParserFactory spf = SAXParserFactory.newInstance();
	    spf.setNamespaceAware(false);	    
		SAXParser saxParser = spf.newSAXParser();
		XMLReader xmlReader = saxParser.getXMLReader();
		xmlReader.setContentHandler(new BagAddrPolygon());
		xmlReader.parse(convertToFileURL("/Users/flokstra/bag-ex.xml"));
		} catch (Exception e) {
			System.out.println("CAHUGHT: "+e); 
		}
	}
	
	//
	//
	//
	
	boolean inAdres = false;
	String currentTag = null;
	
	String gid = null;
	String identificatie = null;
	String oppervlakte = null;
	String status = null;
	String gebruiksdoel = null;
	String openbare_ruimte = null;
	String huisnummer = null;
	String huisletter = null;
	String woonplaats = null;
	String actualiteitsdatum = null;
	String pos = null;
	String bouwjaar = null;
	String pandidentificatie = null;
	String pandstatus = null;
	String posList = null;
	String geometrie = null;
	String pandgeometrie = null;


	private void startAdres() {
		System.out.println("BAP: startAdres()");
		inAdres = true;
		//
		gid = null;
		identificatie = null;
		oppervlakte = null;
		status = null;
		gebruiksdoel = null;
		openbare_ruimte = null;
		huisnummer = null;
		huisletter = null;
		woonplaats = null;
		actualiteitsdatum = null;
		pos = null;
		bouwjaar = null;
		pandidentificatie = null;
		pandstatus = null;
		posList = null;
		geometrie = null;
		pandgeometrie = null;
	}
	
	private void emitAdres() {
		System.out.println("BAP: emitAdres()");
		inAdres = false;
	}
	
	//
	//
	//
	
	public void startDocument() throws SAXException {
		// System.out.println("BAP: startDocument()");
	}

    public void endDocument() throws SAXException {
		// System.out.println("BAP: endDocument()");

    }
	
	public void startElement(String namespaceURI, String localName,
			String qName, Attributes atts) throws SAXException {

		System.out.println("BAP: startElement(): "+qName);

		String name = qName.substring(qName.indexOf(':')+1);
		if ( name.equals("verblijfsobject")) {
			startAdres();
		} else
			currentTag = name;
	}
	
	public void endElement(String namespaceURI, String localName,
			String qName) throws SAXException  {
		// System.out.println("BAP: endElement(): "+qName);
		String name = qName.substring(qName.indexOf(':')+1);
		if ( name.equals("verblijfsobject"))
			emitAdres();
	}
	
	public void characters(char[] ch, int start, int length) throws SAXException  {
		if ( inAdres ) {
			String currentValue = new String(ch,start,length);
			// System.out.println("TAG:"+currentTag+"="+new String(ch,start,length));
			if ( currentTag.equals("gid") ) {
				gid = currentValue;
			} else if ( currentTag.equals("identificatie") ) {
				gid = currentValue;
			} else if ( currentTag.equals("oppervlakte") ) {
				gid = currentValue;
			} else if ( currentTag.equals("status") ) {
				gid = currentValue;
			} else if ( currentTag.equals("gebruiksdoel") ) {
				gid = currentValue;
			} else if ( currentTag.equals("openbare_ruimte") ) {
				gid = currentValue;
			} else if ( currentTag.equals("huisnummer") ) {
				gid = currentValue;
			} else if ( currentTag.equals("huisletter") ) {
				gid = currentValue;
			} else if ( currentTag.equals("woonplaats") ) {
				gid = currentValue;
			} else if ( currentTag.equals("actualiteitsdatum") ) {
				gid = currentValue;
			} else if ( currentTag.equals("pos") ) {
				gid = currentValue;
			} else if ( currentTag.equals("bouwjaar") ) {
				gid = currentValue;
			} else if ( currentTag.equals("pandidentificatie") ) {
				gid = currentValue;
			} else if ( currentTag.equals("pandstatus") ) {
				gid = currentValue;
			} else if ( currentTag.equals("posList") ) {
				gid = currentValue;
			} else if ( currentTag.equals("geometrie") ) {
				gid = currentValue;
			} else if ( currentTag.equals("pandgeometrie") ) {
				gid = currentValue;
			} else {
				// incomplete
			}

		}
	}
	
	public static void main(String[] args) {
		try {
			BagAddrPolygon bap = new BagAddrPolygon( GeoNamesDB.geoNameDBConnection() );
			test_parser();
		}	catch (Exception e) {
			System.out.println("CAUGHT: "+e);
			e.printStackTrace();
		}
    }
}