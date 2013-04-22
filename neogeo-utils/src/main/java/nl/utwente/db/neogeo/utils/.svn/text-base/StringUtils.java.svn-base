package nl.utwente.db.neogeo.utils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Collection;

import nl.utwente.db.neogeo.core.NeoGeoException;

public class StringUtils {
	// Do not make this final, overriding this to add/exclude letters can be useful for third party applications
	public static String REGEX_LETTERS = "a-zA-ZàÀâÂäÄáÁéÉèÈêÊëËìÌîÎïÏòÒôÔöÖùÙûÛüÜçÇ’ñß'";

	/**
	 * Converts the first letter to an upper case letter (when applicable).
	 */
	public static String toFirstUpper(String input) {
		if (input == null || "".equals(input)) {
			return input;
		}

		return ("" + input.charAt(0)).toUpperCase() + input.substring(1);
	}

	/**
	 * Returns true when the input is <code>null</code> or empty after trimming.<br/>
	 * Unlike PHP's is_empty(), 0 will not return true.
	 */
	public static boolean isEmpty(String input) {
		return input == null || "".equals(input.trim());
	}

	public static InputStream stringToInputStream(String input) {
		try {
			return new ByteArrayInputStream(input.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			throw new NeoGeoException("UTF-8 not supported.");
		}
	}

	/**
	 * Source: http://www.androidsnippets.com/get-the-content-from-a-httpresponse-or-any-inputstream-as-a-string
	 */
	public static String inputStreamToString(InputStream inputStream) throws IOException {
		String line;
		StringBuilder total = new StringBuilder();

		// Wrap a BufferedReader around the InputStream
		BufferedReader rd = new BufferedReader(new InputStreamReader(inputStream));

		// Read response until the end
		while ((line = rd.readLine()) != null) {
			total.append(line);
		}

		// Return full string
		return total.toString();
	}

	public static String toCamelCase(String s){
		if (s == null) {
			return null;
		}

		String[] parts = s.split("_");
		String camelCaseString = "";

		for (String part : parts){
			camelCaseString += toFirstUpperRestLower(part);
		}

		camelCaseString = firstLower(camelCaseString);

		return camelCaseString;
	}

	/**
	 * This is to overcome a bug in Hibernate: it cannot find the getter for properties with the second letter as a capital.
	 */
	public static String toHibernateCamelCase(String s) {
		if (s == null) {
			return null;
		}

		s = toCamelCase(s);
		return s.substring(0, 2).toLowerCase() + s.substring(2);
	}

	public static String toFirstUpperRestLower(String s) {
		if (s == null) {
			return null;
		}

		return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
	}

	public static String firstUpper(String s) {
		if (s == null) {
			return null;
		}

		return s.substring(0, 1).toUpperCase() + s.substring(1);
	}

	public static String firstLower(String s) {
		if (s == null) {
			return null;
		}

		return s.substring(0, 1).toLowerCase() + s.substring(1);
	}

	public static int numberOfOccurences(String haystack, String needle) {
		int i = 0;
		int count = 0;

		while ((i = haystack.indexOf(needle)) != -1) {
			haystack = haystack.substring(i + needle.length());
			count++;
		}

		return count;
	}

	public static String ifFilledThen(String possiblySet, String toReturn) {
		if (possiblySet == null || possiblySet.equals("")) {
			return "";
		} else {
			return toReturn;
		}
	}

	public static String implode(String glue, Collection<String> pieces) {
		boolean firstElement = true;
		String result = "";

		for (String piece : pieces) {
			result += (firstElement ? "" : glue) + piece;
			firstElement = false;
		}

		return result;
	}

	public static int getFirstDigitPosition(String s) {
		int result = -1;

		for (int i = 0; i < s.length(); i++) {
			if (Character.isDigit(s.charAt(i))) {
				result = i;
				break;
			}
		}

		return result;
	}

	public static int getNrPrefixOccurences(Collection<String> haystack, String prefix) {
		int result = 0;
		
		for (String string : haystack) {
			if (string.startsWith(prefix)) {
				result++;
			}
		}
		
		return result;
	}
	
	public static String removeXMLWhitespace(String xml) {
		return xml.replaceAll(">[\\W]+<", "><");
	}

}
