package nl.utwente.db.neogeo.scraper.xpathdetectors;

import static nl.utwente.db.neogeo.utils.StringUtils.REGEX_LETTERS;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.utwente.db.neogeo.scraper.utils.PhoneNumberHelper;
import nl.utwente.db.neogeo.scraper.utils.ScraperUtils;

import org.apache.http.client.methods.HttpUriRequest;
import org.apache.log4j.Logger;
import org.w3c.dom.Node;

import com.gargoylesoftware.htmlunit.html.DomNode;
import com.gargoylesoftware.htmlunit.html.HtmlBody;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlScript;
import com.gargoylesoftware.htmlunit.html.HtmlStyle;

public class AddressDetector implements XPathDetector {

	public static final List<Pattern> STREETNAME_HOUSENUMBER_PATTERNS;
	public static List<Pattern> POSTAL_CODE_PATTERNS;
	public static List<String> TEXT_NODE_NAMES;

	private static Logger LOGGER = Logger.getLogger(AddressDetector.class);

	static {
		POSTAL_CODE_PATTERNS = new ArrayList<Pattern>();
		TEXT_NODE_NAMES = new ArrayList<String>();
		STREETNAME_HOUSENUMBER_PATTERNS = new ArrayList<Pattern>();

		// These patterns are evaluated in order
		// TODO: support more countries (UK for example, see
		// http://en.wikipedia.org/wiki/Postcodes_in_the_United_Kingdom)
		// TODO write tests for these patterns
		POSTAL_CODE_PATTERNS.add(Pattern
				.compile("(^|\\D)[0-9]{4}\\s*[A-Z]{2}($|\\s)")); // Dutch
																  // postal
																  // code
		
		POSTAL_CODE_PATTERNS.add(Pattern
				.compile("(^|\\D)[0-9]{5}(\\s{1,}[A-Z]{1,}|$)")); // US/German
															 	  // postal
															 	  // code

		STREETNAME_HOUSENUMBER_PATTERNS.add(Pattern
				.compile("[A-Z][" + REGEX_LETTERS + "\\s\\.-]*\\s[0-9/-]{1,5}[" + REGEX_LETTERS + "]?")); // Downing St. 10/b

		STREETNAME_HOUSENUMBER_PATTERNS.add(Pattern
				.compile("[0-9]{1,5}+[" + REGEX_LETTERS + "/-]?,[\\s]+[A-Z][" + REGEX_LETTERS + "\\s\\.-]*")); // 10/b, Downing St.

		TEXT_NODE_NAMES.add("BR");
		TEXT_NODE_NAMES.add("NOBR");
		TEXT_NODE_NAMES.add("#TEXT");
	}
	
	public String detect(HttpUriRequest request) {
		HtmlPage htmlPage = ScraperUtils.toHtmlPage(ScraperUtils.toWebRequest(request));

		return detect(htmlPage);
	}

	public String detect(HtmlPage htmlPage) {
		preparePageForHarvesting(htmlPage);

		DomNode domNode = null;
		List<DomNode> addressNodes = findAddressNodes(htmlPage);

		LOGGER.debug("Found " + addressNodes.size() + " address element(s).");

		if (addressNodes.isEmpty()) {
			return null;
		}
		if (addressNodes.size() == 1) {
			// Find the container of this address node
			domNode = findContainer(addressNodes.get(0));
		} else {
			// Select common ancestor of addressNodes
			domNode = findCommonAncestor(addressNodes);
		}
		
		return findXPathFor(domNode);
	}

	@SuppressWarnings("unchecked")
	public void preparePageForHarvesting(HtmlPage htmlPage) {
		// Remove SCRIPT tags
		List<HtmlScript> scriptTags = (List<HtmlScript>) htmlPage.getByXPath("//script");

		for (HtmlScript scriptTag : scriptTags) {
			scriptTag.remove();
		}
		
		// TODO: does this affect the functionality of style-based algorithms?
		// Perhaps make a copy of the page instead, if possible
		// Remove STYLE tags
		List<HtmlStyle> styleTags = (List<HtmlStyle>) htmlPage.getByXPath("//style");

		for (HtmlStyle styleTag : styleTags) {
			styleTag.remove();
		}
		
		// Remove HTML comments
		Iterable<DomNode> descendants = htmlPage.getDescendants();
		
		for (DomNode descendant : descendants) {
			if (descendant.getNodeName().toUpperCase().equals("#COMMENT")) {
				descendant.remove();
			}
		}
	}

	private DomNode findContainer(DomNode domNode) {
		return findContainerByTopStructure(domNode);
	}

	private DomNode findContainerByTopStructure(DomNode domNode) {
		DomNode result = domNode;

		if (hasSiblingWithEqualTopStructure(domNode)) {
			result = findContainerByTopStructure(domNode.getParentNode());
		}

		return result;
	}

	private boolean hasSiblingWithEqualTopStructure(DomNode domNode) {
		String topStructure = getTopStructure(domNode);

		for (DomNode sibling : getSiblings(domNode)) {
			if (topStructure.equals(getTopStructure(sibling))) {
				return true;
			}
		}

		return false;
	}

	private List<DomNode> getSiblings(DomNode domNode) {
		List<DomNode> result = new ArrayList<DomNode>();

		for (DomNode possibleSibling : domNode.getParentNode().getChildren()) {
			if (!possibleSibling.equals(domNode)) {
				result.add(possibleSibling);
			}
		}

		return result;
	}

	private String getTopStructure(DomNode domNode) {
		String result = domNode.getNodeName();
		DomNode firstChild = findRealChild(domNode, 0);

		if (firstChild != null) {
			result += "/" + firstChild.getNodeName();

			DomNode secondChild = findRealChild(domNode, 1);

			if (secondChild != null) {
				result += "/" + secondChild.getNodeName();
			}
		}

		return result;
	}

	/**
	 * @param domNode
	 * @param offset
	 * @return non-empty text-nodes, or other nodes
	 */
	private DomNode findRealChild(DomNode domNode, int offset) {
		int count = 0;

		for (DomNode child : domNode.getChildren()) {
			if (!isPlainTextNode(child) || !"".equals(child.getTextContent().trim())) {
				if (count++ == offset) {
					return child;
				}
			}
		}

		return null;
	}

	private boolean isPlainTextNode(DomNode domNode) {
		String nodeName = domNode.getNodeName();

		return TEXT_NODE_NAMES.contains(nodeName);
	}

	public DomNode findCommonAncestor(List<DomNode> nodes) {
		if (nodes.isEmpty()) {
			return null;
		}

		// First node becomes intermediary result
		DomNode result = nodes.get(0);
		nodes.remove(0);
		
		for (DomNode node : nodes) {
			// Now find the common ancestor of the intermediary result and the
			// item
			result = findCommonAncestor(result, node);
		}

		return result;
	}

	public DomNode findCommonAncestor(DomNode nodeOne, DomNode nodeTwo) {
		List<DomNode> nodeOneAncestors = getAncestors(nodeOne, true, false);
		List<DomNode> nodeTwoAncestors = getAncestors(nodeTwo, true, false);

		DomNode result = null;

		for (DomNode ancestor : nodeOneAncestors) {
			if (nodeTwoAncestors.contains(ancestor)) {
				result = ancestor;
				break;
			}
		}

		return result;
	}

	/**
	 * @param DomNode
	 * @param includeSelf
	 * @param topDown
	 *            - true if the list should start at the HTML node, false if it
	 *            should start at the DomNode
	 * @return
	 */
	public List<DomNode> getAncestors(DomNode DomNode, boolean includeSelf,
			boolean topDown) {
		if (DomNode == null) {
			throw new NullPointerException("DomNode shall not be null");
		}

		List<DomNode> result = new ArrayList<DomNode>();

		if (includeSelf) {
			result.add(DomNode);
		}

		DomNode node = DomNode.getParentNode();

		while (node != null) {
			if (topDown) {
				result.add(0, node);
			} else {
				result.add(node);
			}

			node = node.getParentNode();
		}

		return result;
	}

	private List<DomNode> findAddressNodes(HtmlPage htmlPage) {
		HtmlBody body = (HtmlBody) htmlPage.getBody();
		List<DomNode> result = null;

		String bodyText = body.asText();
		result = findAddressNodes(htmlPage, bodyText);

		if (result.isEmpty()) {
			result = findAddressNodes(htmlPage, body.getTextContent());
		}

		return result;
	}

	private List<DomNode> findAddressNodes(HtmlPage htmlPage, String contentToSearchIn) {
		List<String> addressElements = new ArrayList<String>();
		List<DomNode> result = new ArrayList<DomNode>();

		// Phone numbers
		List<String> phoneNumbers = findPhoneNumbers(htmlPage.getBody());
		LOGGER.debug("phoneNumbers = " + phoneNumbers);
		
		if (!phoneNumbers.isEmpty()) {
			// TODO solve this using probabilistics rather than choosing the
			// first one?
			addressElements.add(phoneNumbers.get(0));
		}

		// Postal codes
		List<String> postalCodes = findPostalCodes(htmlPage.getBody());
		LOGGER.debug("postalCodes = " + postalCodes);

		if (!postalCodes.isEmpty()) {
			// TODO solve this using probabilistics rather than choosing the
			// first one?
			addressElements.add(postalCodes.get(0));
		}
		
		for (String addressElement : addressElements) {
			result.add(findContainingNode(htmlPage, addressElement));
		}
		
		return result;
	}

	public DomNode findContainingNode(HtmlPage htmlPage, String string) {
		String xPath = "//*[contains(., '" + string + "')]";
		
		@SuppressWarnings("unchecked")
		List<DomNode> possibleNodes = (List<DomNode>) htmlPage.getByXPath(xPath);
		
		if (possibleNodes.isEmpty()) {
			LOGGER.warn("No items found for " + xPath);
			return null;
		} else {
			return possibleNodes.get(possibleNodes.size() - 1);
		}
	}

	public DomNode findFirstDisplayedNode(List<DomNode> possibleNodes) {
		DomNode result = null;

		for (DomNode possibleNode : possibleNodes) {
			if (possibleNode.isDisplayed()) {
				result = possibleNode;
				break;
			}
		}

		return result;
	}

	public List<String> findPhoneNumbers(HtmlElement htmlElement) {
		List<String> phoneNumberPatternMatches = findPatternMatches(htmlElement, PhoneNumberHelper.PATTERNS);
		List<String> result = new ArrayList<String>();

		for (String potentialPhoneNumber : phoneNumberPatternMatches) {
			if (PhoneNumberHelper.validate(potentialPhoneNumber)) {
				result.add(potentialPhoneNumber);
			}
		}
		
		return result;
	}
	
	public List<String> findPostalCodes(HtmlElement htmlElement) {
		return findPatternMatches(htmlElement, POSTAL_CODE_PATTERNS);
	}
	
	public List<String> findPatternMatches(HtmlElement htmlElement, List<Pattern> orderedPatterns) {
		List<String> result = new ArrayList<String>();

		for (DomNode descendant : htmlElement.getDescendants()) {
			String textContent = descendant.getNodeValue();

			if (textContent == null) {
				continue;
			}

			result.addAll(findPatternMatches(textContent, orderedPatterns));
		}

		return result;
	}

	public boolean isStreetNameAndHouseNumber(String text) {
		for (Pattern pattern : STREETNAME_HOUSENUMBER_PATTERNS) {
			Matcher matcher = pattern.matcher(text);

			if (matcher.matches()) {
				return true;
			}
		}

		return false;
	}

	public List<String> findPatternMatches(String text,
			List<Pattern> orderedPatterns) {
		List<String> result = new ArrayList<String>();

		for (Pattern pattern : orderedPatterns) {
			Matcher matcher = pattern.matcher(text);
			
			while (matcher.find()) {
				result.add(matcher.group().trim());
			}

			if (!result.isEmpty()) {
				break;
			}
		}

		return result;
	}

	private String findXPathFor(DomNode domNode) {
		// TODO improve this, similar to Dolf's approach
		Node idAttributeNode = domNode.getAttributes().getNamedItem("id");

		if (idAttributeNode != null) {
			return "//" + domNode.getNodeName() + "[@id='" + idAttributeNode.getNodeValue() + "']";
		} if (domNode.getNodeName().toUpperCase().equals("HTML")) {
			return "/html";
		} else {
			return findXPathFor(domNode.getParentNode()) + "/" + getNonIDXPathFor(domNode);
		}
	}

	private String getNonIDXPathFor(DomNode domNode) {
		Set<String> classNames = new HashSet<String>();
		classNames.addAll(ScraperUtils.getClassNames(domNode));

		Iterable<DomNode> siblings = domNode.getParentNode().getChildren();

		int index = 1;
		boolean hasSiblingWithSameNodeName = false;
		boolean passedItem = false;

		for (DomNode sibling : siblings) {
			if (sibling.equals(domNode)) {
				passedItem = true;
				continue;
			}

			if (sibling.getNodeName().equals(domNode.getNodeName())) {
				if (!passedItem) {
					index++;
				}

				hasSiblingWithSameNodeName = true;
			}

			List<String> siblingClassNames = ScraperUtils.getClassNames(sibling);

			for (String siblingClassName : siblingClassNames) {
				classNames.remove(siblingClassName);
			}
		}

		if (!classNames.isEmpty()) {
			return domNode.getNodeName() + "[contains(@class, '" + classNames.toArray()[0] + "')]";
		} else {
			return domNode.getNodeName() + (hasSiblingWithSameNodeName ? "[" + index + "]" : "");
		}
	}
}