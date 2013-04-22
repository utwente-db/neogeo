package nl.utwente.db.neogeo.scraper.utils;

import nl.utwente.db.neogeo.core.model.Address;
import nl.utwente.db.neogeo.utils.StringUtils;

public abstract class AddressHelper {
	public static Address parseStreetnameAndHouseNumber(String streetNameAndHouseNumber) {
		Address result = new Address();
		parseStreetnameAndHouseNumber(result, streetNameAndHouseNumber);

		return result;
	}

	public static void parseStreetnameAndHouseNumber(Address address, String streetNameAndHouseNumber) {
		if (Character.isDigit(streetNameAndHouseNumber.charAt(0))) {
			// Format: "10, Downing Street"
			int firstSpacePos = streetNameAndHouseNumber.indexOf(" ");

			address.setHouseNumber(streetNameAndHouseNumber.substring(0, firstSpacePos).replace(",", ""));
			address.setStreetName(streetNameAndHouseNumber.substring(firstSpacePos).trim());
		} else {
			// Format: "Downing Street 10"
			int firstDigitPos = StringUtils.getFirstDigitPosition(streetNameAndHouseNumber);

			address.setHouseNumber(streetNameAndHouseNumber.substring(firstDigitPos));
			address.setStreetName(streetNameAndHouseNumber.substring(0, firstDigitPos).trim());
		}
	}
}
