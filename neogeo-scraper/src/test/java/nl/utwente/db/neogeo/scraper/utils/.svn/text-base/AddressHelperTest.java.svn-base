package nl.utwente.db.neogeo.scraper.utils;

import junit.framework.Assert;
import nl.utwente.db.neogeo.core.model.Address;

import org.junit.Test;

public class AddressHelperTest {
	@Test
	public void parseStreetnameAndHouseNumber() {
		Address iependaal13 = AddressHelper.parseStreetnameAndHouseNumber("Iependaal 13");

		Assert.assertEquals("Iependaal", iependaal13.getStreetName());
		Assert.assertEquals("13", iependaal13.getHouseNumber());

		Address oldenzaalsestraat266 = AddressHelper.parseStreetnameAndHouseNumber("Oldenzaalsestraat 266");

		Assert.assertEquals("Oldenzaalsestraat", oldenzaalsestraat266.getStreetName());
		Assert.assertEquals("266", oldenzaalsestraat266.getHouseNumber());

		Address downingStreet10 = AddressHelper.parseStreetnameAndHouseNumber("10, Downing Street");

		Assert.assertEquals("Downing Street", downingStreet10.getStreetName());
		Assert.assertEquals("10", downingStreet10.getHouseNumber());
	}
}
