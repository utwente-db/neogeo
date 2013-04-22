package nl.utwente.db.neogeo.utils.test;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.Before;

public abstract class NeoGeoUnitTest {
	protected Logger logger = Logger.getLogger(this.getClass());
	
	@Before
	public void setUp() {
		initLogger();
		printInformationHeaderLarge(this.getClass().getSimpleName());
	}

	protected void printInformationHeaderLarge(String header) {
		printInformationHeader(header, "=");
	}
	
	protected void printInformationHeaderSmall(String header) {
		printInformationHeader(header, "-");
	}
		
	protected void printInformationHeader(String header, String fillCharacters) {
		int nrCharactersBefore = (61 - header.length()) / (2 * fillCharacters.length());
		int nrCharactersAfter = (60 - header.length()) / (2 * fillCharacters.length());
		
		String lineToPrint = "";
		
		for (int i = 0; i < nrCharactersBefore; i = i + fillCharacters.length()) {
			lineToPrint += fillCharacters;
		}
		
		lineToPrint += " " + header + " ";
		
		for (int i = 0; i < nrCharactersAfter; i = i + fillCharacters.length()) {
			lineToPrint += fillCharacters;
		}
		
		logger.info(lineToPrint);
	}

	private void initLogger() {
		Layout layout = new PatternLayout("%d{ABSOLUTE} %5p %c{1}:%L - %m%n");
		Appender appender = new ConsoleAppender(layout);
		
		logger.addAppender(appender);
	}
}
