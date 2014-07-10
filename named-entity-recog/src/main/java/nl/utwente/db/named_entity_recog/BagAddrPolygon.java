package nl.utwente.db.named_entity_recog;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import nl.utwente.db.neogeo.twitter.SqlUtils;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class BagAddrPolygon extends DefaultHandler {
	
	Connection c =  null;
	String schema  = null;
	String table = null;
	PreparedStatement insertStat = null;
	PreparedStatement selectStat = null;
	
	public static String MY_SRID = "28992";
	
	private static final boolean sax_verbose = false;
	
	public BagAddrPolygon(Connection c, String schema, String table) throws SQLException {
		this.c = c;
		this.schema = schema;
		this.table = table;
		//
		if ( SqlUtils.existsTable(c, schema, table))
			dropGeoTable(c,schema,table);
		createGeoTable(c,schema,table);
		//
		insert_count = adres_count = 0;
	}
	
	private static String xconvertToFileURL(String filename) {
        String path = new File(filename).getAbsolutePath();
        if (File.separatorChar != '/') {
            path = path.replace(File.separatorChar, '/');
        }

        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return "file:" + path;
    }
	
	public static void scrape_bag_parcels() throws SQLException {
		// 249000,474000 tot 253000,478000. Zie http://epsg.io/28992/map
		int delta;
		int d0_lb, d0_ub, d1_lb, d1_ub;

		if ( false ) {
			delta	= 1000;
			d0_lb	= 250000;
			d0_ub	= 251000;
			d1_lb	= 475000;
			d1_ub	= 476000;
		} else {
			delta	= 500;
			d0_lb	= 249000;
			d0_ub	= 253000;
			d1_lb	= 474000;
			d1_ub	= 478000;
		}
		int d0_count = (int)Math.ceil(((double)d0_ub - (double)d0_lb)/(double)delta);
		int d1_count = (int)Math.ceil(((double)d1_ub - (double)d1_lb)/(double)delta);
		
		BagAddrPolygon bap = new BagAddrPolygon(GeoNamesDB.geoNameDBConnection(),"public","hengelo");
		for(int i=0; i<d0_count; i++)
			for(int j=0; j<d1_count; j++) {
				String url = parcel_url(d0_lb+i*delta, d1_lb+j*delta, d0_lb + (i+1)*delta, d1_lb + (j+1)*delta);
				// url = convertToFileURL("/Users/flokstra/bag-ex.xml");
				LOG.println(url);
				if ( true ) {
					add2database(url, bap);
				}
			}		
	}
	
	public static String parcel_url(int d0_lb, int d1_lb, int d0_ub, int d1_ub) {
		String segment = "BBOX="+d0_lb+","+d1_lb+","+d0_ub+","+d1_ub;
		System.out.println("#!DOING: "+segment);
		String url = "http://geodata.nationaalgeoregister.nl/bag/wfs?&REQUEST=GetFeature&SERVICE=WFS&VERSION=1.1.0&TYPENAME=bag:verblijfsobject&"+segment+"&SRSNAME=EPSG:28992&OUTPUTFORMAT=text%2Fxml%3B%20subtype%3Dgml%2F3.1.1";
		return url;
	}
	
	public static void add2database(String url, BagAddrPolygon bap) {
		try {
			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setNamespaceAware(false);
			SAXParser saxParser = spf.newSAXParser();
			XMLReader xmlReader = saxParser.getXMLReader();
			xmlReader.setContentHandler(bap);
			xmlReader.parse(url);
		} catch (Exception e) {
			LOG.println("CAHUGHT: "+e); 
		}
	}
	
	//
	//
	//
	
	int adres_count = 0;
	int insert_count = 0;

	
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
	String postcode = null;
	String actualiteitsdatum = null;
	String bouwjaar = null;
	String pandidentificatie = null;
	String pandstatus = null;
	String geometrie = null;
	StringBuilder pandgeometrie = null;

	boolean in_geometrie = false;
	boolean in_pandgeometrie = false;

	private void startAdres() {
		if ( sax_verbose ) LOG.println("BAP: startAdres()");
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
		postcode = null;
		actualiteitsdatum = null;
		bouwjaar = null;
		pandidentificatie = null;
		pandstatus = null;
		geometrie = null;
		pandgeometrie = new StringBuilder();
	}
	
	private String strip3d(String geom) {
		try {
		StringBuilder res = new StringBuilder();
		
		int p = 0;
		while (p >= 0) {
			int startp = p;
			if (geom.charAt(p) == ' ')
				return null; // bad format
			p = geom.indexOf(' ', p); // skip over dim 0
			if ( p < 0 )
				return null; // invalid
			p = geom.indexOf(' ', p + 1); // skip over dim 1
			if ( p < 0 )
				return null; // invalid
			res.append(geom.substring(startp, p));
			p = geom.indexOf(' ', p + 1);
			if ( p > 0 ) {
				p++;
				res.append(',');
			}
		}
		if ( false ) {
			LOG.println("+ ["+geom+"]");
			LOG.println("> ["+res+"]");
		}
		return res.toString();
		} catch (Exception e) {
			return null;
		}
	}
	
	private void emitAdres() throws SQLException {
		adres_count++;
		
		this.insertParcel((new Long(identificatie)).longValue(), openbare_ruimte, (new Integer(huisnummer).intValue()), huisletter, postcode, woonplaats,null, strip3d(geometrie), strip3d(pandgeometrie.toString()));
		inAdres = false;
	}
	
	//
	//
	//
	
	public void startDocument() throws SAXException {
		// LOG.println("BAP: startDocument()");
	}

    public void endDocument() throws SAXException {
		// LOG.println("BAP: endDocument()");
    	if ( true ) {
    		LOG.println("#Parsed   "+adres_count+" adresses.");
    		LOG.println("#Inserted "+insert_count+" adresses.");
    	}

    }
	
	public void startElement(String namespaceURI, String localName,
			String qName, Attributes atts) throws SAXException {

		if ( sax_verbose )  LOG.println("BAP: startElement(): "+qName);

		String name = qName.substring(qName.indexOf(':')+1);
		if ( name.equals("verblijfsobject")) {
			startAdres();
		} else if ( name.equals("geometrie")) {
			in_geometrie = true;
		} else if ( name.equals("pandgeometrie")) {
			in_pandgeometrie = true;
		} else
			currentTag = name;
	}
	
	public void endElement(String namespaceURI, String localName,
			String qName) throws SAXException  {
		// LOG.println("BAP: endElement(): "+qName);
		String name = qName.substring(qName.indexOf(':')+1);
		if ( name.equals("verblijfsobject")) {
			try {
				emitAdres();
			} catch (SQLException e) {
				System.err.println("#!EMIT FAILED:");
				e.printStackTrace();
			}
		} else if ( name.equals("geometrie")) {
			in_geometrie = false;
		} else if ( name.equals("pandgeometrie")) {
			in_pandgeometrie = false;
		}
	}
	
	public void characters(char[] ch, int start, int length) throws SAXException  {
		if ( inAdres ) {
			String currentValue = new String(ch,start,length);
			if ( currentTag.equals("gid") ) {
				gid = currentValue;
			} else if ( currentTag.equals("identificatie") ) {
				if ( identificatie == null )
					identificatie = currentValue;
				else {
					identificatie += currentValue;
				}
			} else if ( currentTag.equals("oppervlakte") ) {
				oppervlakte = currentValue;
			} else if ( currentTag.equals("status") ) {
				gid = currentValue;
			} else if ( currentTag.equals("gebruiksdoel") ) {
				status = currentValue;
			} else if ( currentTag.equals("openbare_ruimte") ) {
				if ( openbare_ruimte == null )
					openbare_ruimte = currentValue;
				else {
					openbare_ruimte += currentValue;
				}
			} else if ( currentTag.equals("huisnummer") ) {
				if ( huisnummer == null )
					huisnummer = currentValue;
				else {
					huisnummer += currentValue;
				}
			} else if ( currentTag.equals("huisletter") ) {
				huisletter = currentValue;
			} else if ( currentTag.equals("woonplaats") ) {
				if ( woonplaats == null )
					woonplaats = currentValue;
				else {
					woonplaats += currentValue;
				}
			} else if ( currentTag.equals("postcode") ) {
				if ( postcode == null )
					postcode = currentValue;
				else {
					postcode += currentValue;
				}
			} else if ( currentTag.equals("actualiteitsdatum") ) {
				actualiteitsdatum = currentValue;
			} else if ( currentTag.equals("pos") ) {
				if ( in_geometrie ) {
					if ( geometrie == null )
						geometrie = currentValue;
					else {
						geometrie += currentValue;
					}
					if ( sax_verbose )  LOG.println("#######GEOMETRIE="+geometrie);
				} else 
					throw new RuntimeException("UNEXPECTED: "+currentTag+"="+currentValue);
			} else if ( currentTag.equals("posList") ) {
				if ( in_pandgeometrie ) {
					pandgeometrie.append(currentValue);
					if ( sax_verbose )  LOG.println("#######PANDGEOMETRIE.ADD:"+currentValue);
				} else 
					throw new RuntimeException("UNEXPECTED: "+currentTag+"="+currentValue);
			} else if ( currentTag.equals("bouwjaar") ) {
				bouwjaar = currentValue;
			} else if ( currentTag.equals("pandidentificatie") ) {
				pandidentificatie = currentValue;
			} else if ( currentTag.equals("pandstatus") ) {
				pandstatus = currentValue;
			} else {
				// incomplete
			}
		}
	}
	
	//
	//
	//
	
	private void createGeoTable(Connection c, String schema, String table) throws SQLException {
		if ( SqlUtils.existsTable(c, schema, table) )
			SqlUtils.dropTable(c,schema, table);
		SqlUtils.executeNORES(c, 
				"CREATE TABLE " + schema + "." + table + " (" +
				"id bigint PRIMARY KEY," +
				"street text," +
				"housenumber int," +
				"houseletter varchar(2)," +
				"postalcode varchar(10)," +
				"city varchar(64)," + 
				"province varchar(64)," + 
				"geometrie_raw text," +
				"pandgeometrie_raw text," +
				"full_address text" +
		");");
		SqlUtils.execute(c,
				"SELECT AddGeometryColumn('"+schema+"','"+table+"','geometrie','"+MY_SRID+"','POINT',2);");
		SqlUtils.executeNORES(c,
				"CREATE INDEX ON "+schema+"."+table+" using gist(geometrie);");
		SqlUtils.execute(c,
				"SELECT AddGeometryColumn('"+schema+"','"+table+"','pandgeometrie','"+MY_SRID+"','LINESTRING',2);");
		SqlUtils.executeNORES(c,
				"CREATE INDEX ON "+schema+"."+table+" using gist(pandgeometrie);");
//		SqlUtils.executeNORES(c,
//				"create index lc_hash_tt_city on "+schema+"."+name+" USING hash(lower(city));"
//		);
		this.insertStat = c.prepareStatement("INSERT INTO " + schema + "." + table + "  (" +
				// "postalcode," +
				"id," +
				"street," +
				"housenumber," +
				"houseletter," +
				"postalcode," +
				"city," + 
				"province," + 
				"geometrie_raw," +
				"geometrie," +
				"pandgeometrie_raw," +
				"pandgeometrie," +
				"full_address" +
		") VALUES(?,?,?,?,?,?,?,?,ST_SetSRID(ST_GeomFromText(concat('POINT(',?,')')),28992),?,ST_SetSRID(ST_GeomFromText(concat('LINESTRING(',?,')')),28992),?);");
		this.selectStat = c.prepareStatement(
				"select id,full_address, geometrie_raw, pandgeometrie_raw from "+ schema + "." + table +" where id = ?;"
			);	
	}
	
	private void insertParcel(long id, String street,int housenumber,String houseletter,String postalcode,String city,String province,String geometrie_raw, String pandgeometrie_raw) throws SQLException {
		StringBuilder fulladdr = new StringBuilder();
		
		fulladdr.append(street);
		fulladdr.append(' ');
		fulladdr.append(housenumber);
		if ( houseletter != null )
			fulladdr.append(houseletter);
		fulladdr.append(' ');
		fulladdr.append(city);
		fulladdr.append(' ');
		if ( province != null ) {
			fulladdr.append(' ');
			fulladdr.append(province);
		}
		if ( postalcode != null ) {
			fulladdr.append(' ');
			fulladdr.append(postalcode);
		}
		if (false) {
			LOG.print("- identificatie=" + id);

			LOG.print(", Full Address=" + fulladdr);
			if (true) {
				LOG.print("\n	+ " + geometrie_raw);
				LOG.print("\n	+ " + pandgeometrie_raw);
			}
			LOG.println("");
		}
		if ( (geometrie_raw == null) || (pandgeometrie_raw == null) ) {
			LOG.println("#!SKIP INVALID GEOM: "+fulladdr);
			LOG.println("#@ "+this.geometrie);
			LOG.println("#@ "+this.pandgeometrie);
			return;
		}
		if ( true ) {
			selectStat.setLong(1, id);
			ResultSet rs = selectStat.executeQuery();
			if ( rs.next() ) {
				if (fulladdr.toString().equals(rs.getString("full_address")) && geometrie_raw.equals(rs.getString("geometrie_raw")) && pandgeometrie_raw.equals(rs.getString("pandgeometrie_raw"))) {
					// LOG.println("#! DUPLICATE THE SAME");
				} else {
					LOG.println("#!DUPLICATE DIFFERS: ");
					LOG.println("+ id: " + id);
					LOG.println("+ NEW full address: [" + fulladdr + "]");
					LOG.println("+ @DB full address: ["
							+ rs.getString("full_address")+"]");
					LOG.println("+ NEW geometrie: " + geometrie_raw);
					LOG.println("+ @DB geometrie: "
							+ rs.getString("geometrie_raw"));
					LOG.println("+ NEW pandgeometrie: "
							+ pandgeometrie_raw);
					LOG.println("+ @DB pandgeometrie: "
							+ rs.getString("pandgeometrie_raw"));
				}
				return;
			}
		}
		insertStat.setLong(1, id);
		insertStat.setString(2, street);
		insertStat.setInt(3, housenumber);
		insertStat.setString(4, houseletter);
		insertStat.setString(5, postalcode);
		insertStat.setString(6, city);
		insertStat.setString(7, province);
		insertStat.setString(8, geometrie_raw);
		insertStat.setString(9, geometrie_raw);
		insertStat.setString(10, pandgeometrie_raw);
		insertStat.setString(11, pandgeometrie_raw);
		insertStat.setString(12, fulladdr.toString());
		try {
			insertStat.execute();
		} catch (SQLException e) {
			System.err.println("CAUGHT: "+e);
			System.err.println("FULL ADDRESS="+fulladdr);
			e.printStackTrace();
			LOG.println(""+insertStat);
			// System.exit(-1);
		}
		//
		insert_count++;
	}
	
	private static void dropGeoTable(Connection c, String schema, String table) throws SQLException {
		try {
			SqlUtils.execute(c,
				"SELECT DropGeometryTable('"+schema+"','"+table+"');"); 
			} catch (SQLException e) {
				LOG.println("IGNORE: "+e);
			}
	}
		
	//
	//
	//
	
	private static PrintStream LOG = System.out;
	
	public static void main(String[] args) {
		try {
			// LOG = new PrintStream(new File ("/Users/flokstra/LOG.HENGELO"));
			scrape_bag_parcels();
		}	catch (Exception e) {
			LOG.println("CAUGHT: "+e);
			e.printStackTrace();
		}
    }
}