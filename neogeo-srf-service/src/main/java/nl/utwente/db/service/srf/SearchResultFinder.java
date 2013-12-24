package nl.utwente.db.service.srf;

import java.util.ArrayList;
import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;

public class SearchResultFinder {
	private WebDriver d;
	private JavascriptExecutor jse;
	private String scriptUrl;
	
	/**
	 * Creates a new SearchResultFinder
	 * @param scriptUrl - url pointing to searchresultfinder main.js file
	 */
	public SearchResultFinder(String scriptUrl) {
		this.scriptUrl = scriptUrl;
	}
	
	private void createDriver() {
		this.d = new FirefoxDriver();
		this.jse = (JavascriptExecutor) this.d;
	}
	
	private void closeDriver() {
		this.d.close();
	}
	
	private String getInjectionScript() {
		return
			"var head = document.getElementsByTagName('head')[0]; " +
	        "var script = document.createElement('script'); " +
	        "script.type = 'text/javascript'; " +
	        "script.src = '" + this.scriptUrl + "';" +
	        "head.appendChild(script);";
	}
	
	
	public List<String> search(String url) {
		createDriver();
		try {
			// open the page
			d.get(url);

			// inject script for running searchresultfinder
			jse.executeScript(getInjectionScript());
			
			// wait for dialog to appear
	        ExpectedCondition<Boolean> condition = new ExpectedCondition<Boolean>() {
	        	// hier een override weggehaald, anders werkte het niet
	            public Boolean apply(final WebDriver webDriver) {
	                WebElement element = webDriver.findElement(By.id("srf_dialog"));
	                return element != null;
	            }
	        };			
			WebDriverWait w = new WebDriverWait(d, 20);
			w.until(condition);
			
			// extract the xpaths from the dialog
			List<String> result = new ArrayList<String>();
			for (WebElement we : d.findElements(By.xpath("//table[@id='srf_xpathtable']//tr[position() = 3]//td[2]"))) {
				result.add(we.getText());
			}			
			return result;
		} catch (TimeoutException toe) {
			return null;
		} finally {
			closeDriver();
		}
	}
	
	public static void main(String[] args) {
		SearchResultFinder srf = new SearchResultFinder("http://castle.ewi.utwente.nl/srf/main.js");
		for (String xpath: srf.search("http://dolf.trieschnigg.nl")) {
			System.out.println(xpath);
		}
	}
	
	
}
