package nl.utwente.db.neogeo.twitter.harvest.type;

import nl.utwente.db.neogeo.twitter.harvest.type.geo.BoundingBox;

/** 
 * 
 * https://dev.twitter.com/docs/api/1/get/geo/reverse_geocode
 * 
 */

public class Place implements interfaceType{
	public String m_name;
	public String m_country;
	public String m_countryCode;
	public String m_attributes;
	public String m_url;
	public String m_id;
	public BoundingBox m_boundingBox;
	public Place m_containedWithin;
	
	public String pirnt() {
		StringBuilder sbPrint = new StringBuilder();
		sbPrint.append("Place" + System.getProperty("line.separator"));
		sbPrint.append("name = " + m_name + System.getProperty("line.separator"));
		sbPrint.append("id = " + m_id + System.getProperty("line.separator"));
		sbPrint.append("country = " + m_country + System.getProperty("line.separator"));
		sbPrint.append("countryCode = " + m_countryCode + System.getProperty("line.separator"));
		sbPrint.append("=====================" + System.getProperty("line.separator"));
		String strPrint = sbPrint.toString();
		System.out.print(strPrint);
		return strPrint;
	}
	
}
