package nl.utwente.db.neogeo.utils;

import java.io.ByteArrayInputStream;

import org.w3c.dom.*;
import javax.xml.parsers.*;
import javax.xml.xpath.*;

public class XPathUtils {

	public static String[] xpathOnString(String q, String stringdoc) {
		try {
			DocumentBuilderFactory domFactory = DocumentBuilderFactory
					.newInstance();
			// domFactory.setNamespaceAware(true);
			DocumentBuilder builder = domFactory.newDocumentBuilder();
			Document doc = builder.parse(new ByteArrayInputStream(stringdoc
					.getBytes()));

			XPathFactory factory = XPathFactory.newInstance();
			XPath xpath = factory.newXPath();
			XPathExpression expr = xpath.compile(q);

			Object result = expr.evaluate(doc, XPathConstants.NODESET);
			NodeList nodes = (NodeList) result;

			String res[] = new String[nodes.getLength()];
			for (int i = 0; i < nodes.getLength(); i++) {
				// System.out.println(nodes.item(i).toString());
				res[i] = nodes.item(i).getNodeValue();
			}
			return res;
		} catch (Exception e) {
			System.out.println("XPathUtils.xpathOnString:caught:"+e);
			return null;
		}
	}

}