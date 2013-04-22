package nl.utwente.db.neogeo.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import nl.utwente.db.neogeo.core.NeoGeoException;
import nl.utwente.db.neogeo.core.model.Address;

public class GeoUtils {

	// Locale.getISOCountries two letter code for all countries
	
	static final boolean verbose = true;
	
	public static double[] geocode(String country, String address) {
		if (country != null && country.length()==2) {
			if (country.equals("nl"))
				return get_geocode_nrg(address,null);
		}
		return get_geocode_nrg(address,null);
	}
	
	public static String encodeAddress_nrg(String town, String street, String number, String sub, String pcode) {
		if (town != null) {
			if (town.contains(" gem")) {
				int ioc = town.indexOf(" gem");
				if (verbose)
					System.out.println("geo_nrg: stripping ["+town.substring(ioc)+"] from "
							+ town);
				town = town.substring(0, ioc);
			}
			if (town.endsWith(" ov")) {
				if (verbose)
					System.out.println("geo_nrg: stripping [ ov] from " + town);
				town = town.substring(0, town.length() - 3);
			}
			if (town.endsWith(" gld")) {
				if (verbose)
					System.out
							.println("geo_nrg: stripping [ gld] from " + town);
				town = town.substring(0, town.length() - 4);
			}
			if (town.endsWith(" gn")) {
				if (verbose)
					System.out.println("geo_nrg: stripping [ gn] from " + town);
				town = town.substring(0, town.length() - 3);
			}
			if (town.endsWith(" nb")) {
				if (verbose)
					System.out.println("geo_nrg: stripping [ nb] from " + town);
				town = town.substring(0, town.length() - 3);
			}
			if (town.endsWith(" zh")) {
				if (verbose)
					System.out.println("geo_nrg: stripping [ zh] from " + town);
				town = town.substring(0, town.length() - 3);
			}
			if (town.endsWith(" nh")) {
				if (verbose)
					System.out.println("geo_nrg: stripping [ nh] from " + town);
				town = town.substring(0, town.length() - 3);
			}
		}
	
		StringBuffer result = new StringBuffer();
		try {
			if ( town != null ) {
				town   = URLEncoder.encode(town, "UTF-8");
				result.append(town);
			}
			if ( street != null ) {
				street = URLEncoder.encode(street, "UTF-8");
				if ( result.length() > 0 )
					result.append("+");
				result.append(street);
			}
		} catch (UnsupportedEncodingException e) {
			throw new NeoGeoException("UTF-8 not supported.");
		}		
		if ( result.length() > 0 )
			result.append("+");
		result.append(number);
		if (pcode != null)
			result.append("+" + pcode.replace(' ','+'));
		return result.toString();
	}
	
	public static double[] geocode(String country, Address address) {
		String town = address.getTown().getName();
		String street = address.getStreetName();
		String number = address.getHouseNumber();
		String sub = null;
		if ( (number != null) && (number.indexOf('/') >= 0) ) {
			int sep = number.indexOf('/');
			sub = number.substring(sep + 1);
			number = number.substring(0, sep);
		}
		String pcode = address.getPostalCode();
		
		if ( town == null || street == null || number == null ) {
			return null;
		}

		double[] res = null;
		String as = encodeAddress_nrg(town, street, number, sub, pcode);
		if (as != null) {
			res = get_geocode_nrg(as, sub);
			if ( res == null ) {
				// try it with just the postal code and the number
				as = encodeAddress_nrg(null, null, number, sub, pcode);
				if ( as!= null )
					res = get_geocode_nrg(as, sub);
			}
//			if ( (res == null) && (sub == null) ) {
//				// try finding the neighbours
//				int thisnum;
//				try {
//					thisnum = Integer.parseInt(number,10);
//				} catch (NumberFormatException e) {
//					thisnum = -1;
//				}
//				if ( thisnum >= 2 ) {
//					int lnum = thisnum - 2;
//					as = encodeAddress_nrg(town, street, ""+lnum, sub, pcode);
//					double lres[] = get_geocode_nrg(as, sub);
//					if ( lres != null ) {
//						System.out.println("#! XXX: found a lower neighbour");
//						int hnum = thisnum + 2;
//						as = encodeAddress_nrg(town, street, ""+hnum, sub, pcode);
//						double hres[] = get_geocode_nrg(as, sub);
//						if (hres != null) {
//							System.out.println("#! YYY: also found a higher neighbour");
//						}
//					}
//					
//				}
//			}
		}
		return res;
	}
	
	/*
	 * 
	 * 
	 */
	
	public static double[] get_geocode_nrg(String address, String sub) {
		if (verbose)
			System.out.println("geo_nrg: address=" + address);		
		String nrg_url = "http://geodata.nationaalgeoregister.nl/geocoder/Geocoder?zoekterm="
				+ address + "&strict=true";
		
		if (verbose)
			System.out.println("geo_nrg: nrg_url=" + nrg_url);
		String doc = WebUtils.getContent(nrg_url);
		if (false && verbose)
			System.out.println("geo_nrg: doc=\n" + doc);
		if (doc == null || doc.length() == 0) {
			if (verbose)
				System.out.println("geo_nrg: ERROR NO XML RESULT");
			return null;
		}
		String[] xpres = XPathUtils.xpathOnString(
				"//GeocodedAddress/Point/pos/text()", doc);
		if ((xpres != null) && (xpres.length > 0)) {
			if (xpres.length > 1) {
				/* more geolocations are returned. Try to get a close match of the
				 * specified subdivision. When a subdivision was specified try the
				 * upper and lower case variants. When no subdivision was specified 
				 * try the Address/.../Building without a subdivision attribute. When
				 * this fails just take the first returned (WARNING: may be inaccurate)
				 */
				String[] docres;
				if (sub != null) {
					docres = XPathUtils.xpathOnString(
							"//GeocodedAddress[./Address/StreetAddress/Building[@subdivision='"
									+ sub.toUpperCase()
									+ "']]/Point/pos/text()", doc);
					if (docres.length == 0) {
						docres = XPathUtils.xpathOnString(
								"//GeocodedAddress[./Address/StreetAddress/Building[@subdivision='"
										+ sub.toLowerCase()
										+ "']]/Point/pos/text()", doc);
					}
				} else {
					docres = XPathUtils
							.xpathOnString(
									"//GeocodedAddress[./Address/StreetAddress/Building[not(@subdivision)]]/Point/pos/text()",
									doc);
				}
				if (docres.length > 0 && docres.length < xpres.length) {
					if (verbose)
						System.out
								.println("geo_nrg: selecting addresses for subdivision["
										+ sub + "]");
					xpres = docres;
				}
			}
			if (verbose && xpres.length > 1)
				System.out
						.println("geo_nrg: WARNING UNHANDLED MULTIPLE RESULTSo[sub="
								+ sub + "]");

			String d12 = xpres[0];
			int sep = d12.indexOf(" ");
			Double d1 = new Double(d12.substring(0, sep));
			Double d2 = new Double(d12.substring(sep + 1));

			double res[] = { d1.doubleValue(), d2.doubleValue() };
			if (verbose)
				System.out.println("geo_nrg: res=[" + res[0] + "," + res[1]
						+ "]");
			return res;

		}
		if (verbose)
			System.out.println("geo_nrg: NO RESULT");
		return null;
	}

	public static void main(String[] argv) {
		System.out.println("WebUtils main:");
		String addr = encodeAddress_nrg("Enschede", "Colosseum", "101", null, "7521PP");
		// double geocode[] = GeoUtils.geocode("nl",addr);
		double geocode[] = GeoUtils.get_geocode_nrg(addr,null);

		if ( geocode == null )
			System.out.println("No geocode for address:"+addr);
		else 
			System.out.println("Geocode["+addr+"]={"+geocode[0]+" "+geocode[1]+"}");
	}
}
