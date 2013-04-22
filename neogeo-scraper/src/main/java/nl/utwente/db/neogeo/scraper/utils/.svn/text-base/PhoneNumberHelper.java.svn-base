package nl.utwente.db.neogeo.scraper.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import nl.utwente.db.neogeo.utils.StringUtils;

public class PhoneNumberHelper {
	public final static List<Pattern> PATTERNS;

	static {
		PATTERNS = new ArrayList<Pattern>();

		PATTERNS.add(Pattern.compile("\\+[1-9][0-9\\s()-\\/]*"));
		PATTERNS.add(Pattern.compile(   "[0-9][0-9()\\s-\\/]{6,}"));
	}

	public static boolean validate(String potentialPhoneNumber) {
		return(   StringUtils.numberOfOccurences(potentialPhoneNumber, "-") <= 1	// No more than one dash
				&& StringUtils.numberOfOccurences(potentialPhoneNumber, "/") <= 1	// No more than one slash
				&& !potentialPhoneNumber.matches("[0-9]{4}\\s*-\\s*[0-9]{4}")		// No 0000 - 0000 format, this is probably a year combination
				&& !potentialPhoneNumber.contains("\n")								// No new lines inside the phone number, this is probably a connection of two 'unrelated' numbers
				&& potentialPhoneNumber.replaceAll("\\D", "").length() > 5);		// At least 5 digits
	}
}
