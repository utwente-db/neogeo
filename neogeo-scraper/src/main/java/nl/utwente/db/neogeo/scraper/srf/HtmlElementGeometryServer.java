package nl.utwente.db.neogeo.scraper.srf;

import java.util.List;

import java.util.Vector;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.*;
import org.openqa.selenium.interactions.Action;
import org.openqa.selenium.interactions.Actions;


public class HtmlElementGeometryServer {
	
	// this first implementation first only implements firefox
	
	private FirefoxDriver driver;
	private	HtmlElementGeometry top_elem;
	
	public HtmlElementGeometryServer() {
		this.driver	= new FirefoxDriver();
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
	
	String js_head =
		"var xpt = \'";
	String js_foot = "\';\n"
	        + "var nodesSnapshot = document.evaluate(xpt, document, null,\n"
			+ "         XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null );\n"
			+ "for ( var i=0 ; i < nodesSnapshot.snapshotLength; i++ )  \n"
			+ "{  \n"
			+ "  nodesSnapshot.snapshotItem(i).style.color = \"#ff0000\"; \n"
			+ "  nodesSnapshot.snapshotItem(i).style.backgroundColor = \"##98FB98\";\n"
			+ "  nodesSnapshot.snapshotItem(i).style.outline = \'#f00 solid 2px\';\n"
			+ "}  \n"
			;

	public void highlight(String xpath) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(js_head);
		for(int i=0; i<xpath.length(); i++) {
			if ( xpath.charAt(i) == '\'')
				sb.append('\\');
			sb.append(xpath.charAt(i));
		}
		sb.append(js_foot);
		// System.out.println("JS-EXECUTE:\n"+sb.toString());
		driver.executeScript(sb.toString());
		//
		getGeometry(xpath);
	}
	
}
