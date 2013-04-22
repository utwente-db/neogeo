package nl.utwente.db.neogeo.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import nl.utwente.db.neogeo.core.NeoGeoException;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.util.ClassUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.tidy.Tidy;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.json.JettisonMappedXmlDriver;
import com.thoughtworks.xstream.io.json.JsonHierarchicalStreamDriver;
import com.thoughtworks.xstream.io.json.JsonWriter;

public abstract class WebUtils {
	public static String getContent(String url) {
		String result = "";
		InputStream inputStream = getInputStream(url);

		try {
			InputStreamReader isReader = new InputStreamReader(inputStream);
			BufferedReader reader = new BufferedReader(isReader);
			String line;

			while ((line = reader.readLine()) != null) {
				result += line;
			}

			reader.close();
		}  catch (IOException e) {
			throw new NeoGeoException(e);
		}

		return result;
	}

	public static InputStream getInputStream(String url) {
		return getInputStream(url, 5);
	}

	public static InputStream getInputStream(String url, int nrRetries) {
		try {
			URL javaUrl = new URL(url);

			return javaUrl.openStream();
		} catch (MalformedURLException e) {
			throw new NeoGeoException(e);
		} catch (IOException e) {
			if (--nrRetries == 0) {
				throw new NeoGeoException(e);
			} else {
				return getInputStream(url, nrRetries);
			}
		}
	}

	public static Object jsonUrlToJava(String url, Object root) {
		String json = getContent(url);
		XStream xStream = new XStream(new JettisonMappedXmlDriver());
		
		return xStream.fromXML(json, root);
	}
	
	public static JSONObject jsonURLToSimpleJSONObject(String url) {
		JSONParser parser = new JSONParser();
		String jsonResponse = WebUtils.getContent(url);
		
		JSONObject object;
		
		try {
			object = (JSONObject)parser.parse(jsonResponse);
		} catch (ParseException e) {
			throw new NeoGeoException("Unable to parse Facebook response for URL " + url, e);
		}
		
		return object;
	}
	
	public static String javaToJSON(Object object) {
		return javaToJSON(object, true);
	}
	
	public static String javaToJSON(Object object, boolean useRootNode) {
		XStream xStream;
		
		if (useRootNode) {
			xStream = new XStream(new JsonHierarchicalStreamDriver());
		} else {
			xStream = new XStream(new JsonHierarchicalStreamDriver() {
			    public HierarchicalStreamWriter createWriter(Writer writer) {
			        return new JsonWriter(writer, JsonWriter.DROP_ROOT_MODE);
			    }
			});
		}

		// Yes, this outputs JSON :)
		return xStream.toXML(object);
	}

	public static void saveUrlContents(String url, String fileName) {
		String contents = getContent(url);
		FileUtils.writeFile(fileName, contents);
	}

	public static Document parseHTML(String html) {
		return parseHTML(StringUtils.stringToInputStream(html));
	}

	public static Document parseHTML(InputStream htmlStream) {
		Tidy tidy = new Tidy();

		tidy.setQuiet(true);
		tidy.setShowWarnings(false);

		return tidy.parseDOM(htmlStream, null);
	}

	public static NodeList getElementsByXPath(Node node, String xPathString) {
		XPathFactory xPathFactory = XPathFactory.newInstance();
		XPath xPath = xPathFactory.newXPath();

		NodeList searchResults = null;

		try {
			XPathExpression xPathExpression = xPath.compile(xPathString);
			searchResults = (NodeList)xPathExpression.evaluate(node, XPathConstants.NODESET);
		} catch (XPathExpressionException e) {
			throw new NeoGeoException("Unable to handle xPath '" + xPathString + "'", e);
		}

		return searchResults;
	}

	public static String getLinkedURL(Node linkNode) {
		return getAttributeValue(linkNode, "href");
	}

	public static String getAttributeValue(Node node, String attributeName) {
		Node namedItem = node.getAttributes().getNamedItem(attributeName);

		if (namedItem == null) {
			return null;
		} else {
			return namedItem.getNodeValue();
		}
	}

	public static Document toDocument(HttpUriRequest request) {
		HttpResponse response = null;
		HttpClient client = new DefaultHttpClient();

		try {
			response = client.execute(request);
		} catch (IOException e) {
			throw new NeoGeoException("IOException while executing request " + request, e);
		}

		InputStream responseStream;

		try {
			responseStream = response.getEntity().getContent();
		} catch (IllegalStateException e) {
			throw new NeoGeoException("IllegalStateException while executing request " + request, e);
		} catch (IOException e) {
			throw new NeoGeoException("IOException while executing request " + request, e);
		}

		return WebUtils.parseHTML(responseStream);
	}

	public static Node findMostImportantHeader(Node node) {
		Node result = null;

		for (int i = 1; i < 7; i++) {
			result = WebUtils.getFirstXPathMatch(node, "//h" + i);

			if (result != null) {
				break;
			}
		}

		if (result == null) {
			result = WebUtils.getFirstXPathMatch(node, "//span");
		}

		if (result == null) {
			result = WebUtils.getFirstXPathMatch(node, "//b");
		}

		return result;
	}

	public static Node getFirstXPathMatch(Node parentNode, String xPath) {
		NodeList possibleResults = WebUtils.getElementsByXPath(parentNode, xPath);
		Node result = null;

		if (possibleResults.getLength() > 0) {
			result = possibleResults.item(0);
		}

		return result;
	}

	public static String getLinkedURL(URL originalURL, String linkedPath) {
		if (linkedPath.startsWith("/")) {
			String portSuffix = originalURL.getPort() == -1 ? "" : ":" + originalURL.getPort();
			linkedPath = originalURL.getProtocol() + "://" + originalURL.getHost() + portSuffix + linkedPath;
		} else if (!linkedPath.startsWith("http")) {
			String originalUrlString = originalURL.toString();
			linkedPath = originalUrlString.substring(0, originalUrlString.lastIndexOf("/") + 1) + linkedPath;
		}

		return linkedPath;
	}

	public static String javaToHTML(Object object) {
		return javaToHTML(object, null, null);
	}
	
	public static String javaToHTML(Object object, Class<?> parentClass, String variableName) {
		if (object == null) {
			return "";
		}
		
		if (ClassUtils.isPrimitiveOrWrapper(object.getClass()) || object instanceof String) {
			return object.toString();
		}
		
		if (object instanceof URL || object instanceof URI) {
			return "<a href=\"" + object + "\">" + object + "</a>";
		}
		
		if (object instanceof Collection) {
			return collectionToHTML((Collection<?>)object, true, parentClass, variableName);
		}
		
		String result = "<table><tr class=\"headers\"><th colspan=\"2\">" + object.getClass().getSimpleName() + "</th></tr>";
		BeanWrapper beanWrapper = new BeanWrapperImpl(object);
		
		for (String childVariableName : SpringUtils.getReadableVariableNames(object)) {
			if (object.getClass().getPackage() == null || "java.lang".equals(object.getClass().getPackage().getName())) {
				continue;
			}
			
			String value = javaToHTML(beanWrapper.getPropertyValue(childVariableName), object.getClass(), childVariableName);
			String propertyType = beanWrapper.getPropertyType(childVariableName).getSimpleName();
			
			result += "<tr class=\"" + childVariableName + " " + propertyType + "\"><td>" + childVariableName + "</td><td>" + value + "</td></tr>";
		}
		
		result += "</table>";
		
		return result;
	}
	
	public static String collectionToHTML(Collection<?> collection, boolean wrapWithTable) {
		return collectionToHTML(collection, wrapWithTable, null, null);
	}

	public static String collectionToHTML(Collection<?> collection, boolean wrapWithTable, Class<?> collectedType) {
		List<String> propertyNames = SpringUtils.getWritableVariableNamesForBeanWrapper(new BeanWrapperImpl(collectedType));
		String result = "";

		if (wrapWithTable) {
			// VG: Sorry about this personal preference ;-)
			result += "<table cellspacing=\"0\"><tr>";
			BeanWrapper collectedObjectWrapper = new BeanWrapperImpl(collectedType);

			for (String propertyName : propertyNames) {
				String propertyType = collectedObjectWrapper.getPropertyType(propertyName).getSimpleName();
				result += "<th class=\"" + propertyType + "\">" + StringUtils.toFirstUpper(propertyName) + "</th>";
			}
			
			result += "</tr>";
		}
		
		for (Object collectedObject : collection) {
			// rows
			result += "<tr>";
			
			BeanWrapper collectedObjectWrapper = new BeanWrapperImpl(collectedObject);
			
			for (String propertyName : propertyNames) {
				String propertyType = collectedObjectWrapper.getPropertyType(propertyName).getSimpleName();

				// columns
				result += "<td class=\"" + propertyName + " " + propertyType + "\">" + javaToHTML(collectedObjectWrapper.getPropertyValue(propertyName)) + "</td>";
			}
			
			result += "</tr>";
		}
		
		if (wrapWithTable) {
			result += "</table>";
		}
		
		return result;
	}
	
	private static String collectionToHTML(Collection<?> collection, boolean wrapWithTable, Class<?> parentClass, String variableName) {
		Class<?> collectedType = CollectionUtils.getTypeOfFirstElement(collection);
		
		if (collectedType == null && parentClass != null && !StringUtils.isEmpty(variableName)) {
			try {
				Field stringListField = parentClass.getDeclaredField(variableName);
				Object collectionType = stringListField.getGenericType();
				
				if (collectionType instanceof ParameterizedType) {
					collectedType = (Class<?>)((ParameterizedType)collectionType).getActualTypeArguments()[0];
				}
			} catch (NoSuchFieldException e) {
				throw new NeoGeoException("No such field: " + variableName + " in " + parentClass, e);
			}
		}
			
		if (collectedType == null) {
			return "";
		}
		
		return collectionToHTML(collection, wrapWithTable, collectedType);
	}
	
	public static String getHostName(String url) {
		try {
			return new URL(url).getHost();
		} catch (MalformedURLException e) {
			throw new NeoGeoException(e);
		}
	}

}
