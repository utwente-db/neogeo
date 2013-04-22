package nl.utwente.db.neogeo.scraper.srf;

import java.util.List;
import java.util.Vector;

import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.remote.RemoteWebElement;

public class DolfSrf {
	
	// this first implementation first only implements firefox
	
	private FirefoxDriver driver;
	private	HtmlElementGeometry top_elem;
	
	public DolfSrf() {
		this.driver	= new FirefoxDriver();
	}
	
	public void getSrfList(String url) {
		driver.get(url);
		// driver.executeAsyncScript("return !!searchresultfinder.run()");
		// driver.executeScript("return 'hello'");
		RemoteWebElement top = (RemoteWebElement)driver.findElement(By.xpath("/html//a"));


		driver.getMouse().mouseDown(top.getCoordinates());
		// driver.getKeyboard().releaseKey(Keys.CONTROL);
		// driver.getMouse().mouseUp(top.getCoordinates());
		
		top.click();

		
//		driver.getKeyboard().pressKey(Keys.ARROW_DOWN);		
//		driver.getKeyboard().releaseKey(Keys.ARROW_DOWN);
//		driver.getKeyboard().pressKey(Keys.ARROW_DOWN);		
//		driver.getKeyboard().releaseKey(Keys.ARROW_DOWN);
//		driver.getKeyboard().pressKey(Keys.ARROW_DOWN);		
//		driver.getKeyboard().releaseKey(Keys.ARROW_DOWN);
//		driver.getKeyboard().pressKey(Keys.ARROW_DOWN);		
//		driver.getKeyboard().releaseKey(Keys.ARROW_DOWN);
//		driver.getKeyboard().pressKey(Keys.ARROW_DOWN);		
//		driver.getKeyboard().releaseKey(Keys.ARROW_DOWN);
//		driver.getKeyboard().pressKey(Keys.ARROW_DOWN);		
//		driver.getKeyboard().releaseKey(Keys.ARROW_DOWN);
//		driver.getKeyboard().pressKey(Keys.ENTER);		
//		driver.getKeyboard().releaseKey(Keys.ENTER);







	
		driver.getKeyboard().pressKey(Keys.ENTER);


		
		
	}
	public void open(String url) {
		driver.get(url);
		WebElement top = driver.findElement(By.xpath("/html"));
		top_elem = new HtmlElementGeometry(top.getLocation().x,top.getLocation().y,top.getSize().getHeight(),top.getSize().getWidth());
	}
	
	public void close() {
		driver.close();
	}
	
	public HtmlElementGeometry topElement() {
		return top_elem;
	}
	public Vector<HtmlElementGeometry> getGeometry(String xpath) {
		Vector<HtmlElementGeometry> res = new Vector<HtmlElementGeometry>();
	
		List<WebElement> l = driver.findElements(By.xpath(xpath));
	    for(int i=0; i<l.size(); i++) {
	    	WebElement wel = l.get(i);
	    	res.add(new HtmlElementGeometry(wel.getLocation().x,wel.getLocation().y,wel.getSize().getHeight(),wel.getSize().getWidth()));
	    }
	    return res;	
	}

}