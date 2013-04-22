package nl.utwente.db.neogeo.utils;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import nl.utwente.db.neogeo.utils.StringUtils;

import org.junit.Test;

public class StringUtilsTest {
	@Test
	public void toFirstUpper() {
		assertEquals(StringUtils.toFirstUpper(null), null);
		assertEquals(StringUtils.toFirstUpper(""), "");
		assertEquals(StringUtils.toFirstUpper("a"), "A");
		assertEquals(StringUtils.toFirstUpper("A"), "A");
		assertEquals(StringUtils.toFirstUpper("aap"), "Aap");
	}
	
	@Test
	public void isEmpty() {
		assertEquals(StringUtils.isEmpty(null), true);
		assertEquals(StringUtils.isEmpty(""), true);
		assertEquals(StringUtils.isEmpty(" "), true);
		
		assertEquals(StringUtils.isEmpty("0"), false);
		assertEquals(StringUtils.isEmpty("&nbsp;"), false);
	}

	@Test
	public void implode() {
		List<String> pieces = new ArrayList<String>();
		
		pieces.add("aap");
		assertEquals("aap", StringUtils.implode(",", pieces));
		
		pieces.add("beer");
		assertEquals("aap;beer", StringUtils.implode(";", pieces));

		pieces.add("clown");
		assertEquals("aap:beer:clown", StringUtils.implode(":", pieces));
	}
}
