/************* HELPER FUNCTIONS *********************/
function cl(message) {
	if (console != undefined) {
		console.log(message);
	}
}

function countNrOccurences(haystack, needle) {  
    return haystack.split(needle).length - 1;
}

function findFirstRealChild(node) {
	return findRealChild(node, 1);
}

function findSecondRealChild(node) {
	return findRealChild(node, 2);
}

function findRealChild(node, nthItem) {
	var results = new Array();
	var children = $(node).contents();

	for (var i = 0; i < children.length; i++) {
		var child = children[i];
		
		if (child.textContent.trim() != '' || 
		    (child.nodeName.toUpperCase() != '#TEXT' && 
		     child.nodeName.toUpperCase() != 'NOBR' &&
		     child.nodeName.toUpperCase() != 'BR')) {
			results[results.length] = child;

			if (results.length == nthItem) {
				break;
			}
		}
	}

	return results[results.length - 1];
}

function getTopStructure(node) {
	cl(node);
	var firstNonTextChild = findFirstRealChild(node);
	var postfix = firstNonTextChild == undefined ? '' : firstNonTextChild.nodeName;

	if (firstNonTextChild == undefined) {
		var secondNonTextChild = findSecondRealChild(node);
		postfix += secondNonTextChild == undefined ? '' : secondNonTextChild.nodeName;
	}
	
	var result = node.nodeName + postfix;
	cl(result);
	return result;
}

function hasSiblingWithSameTopStructure(node) {
	var structure = getTopStructure(node);
	var result = false;

	$(node).siblings().each(function(index, sibling) {
		if (getTopStructure(sibling) == structure) {
			result = true;
		}
	});

	return result;
}

/**
 * Source: http://viralpatel.net/blogs/2011/02/jquery-get-text-element-without-child-element.html
 */
function getTextWithoutChildren(node) {
	return $(node)
		    .clone()    //clone the element
		    .children() //select all the children
		    .remove()   //remove all the children
		    .end()  	//again go back to selected element
		    .text();    //get the text of element
}

function getTextChildren(node) {
	return $(node).contents('p, a, b, i, u, font, span');
}

function isTextContainer(node) {
	return getTextChildren(node).length > 0 || getTextWithoutChildren(node).replace(/\s/g, '').length != 0;
}

function isVisible(node) {
	var display = $(node).css('display');

	return display != 'none' && ($(node)[0].nodeName.toUpperCase() == 'HTML' || isVisible($(node).parent()));
}

function findAncestorsIncludingSelf(node) {
	return jQuery.merge(new Array(node), $(node).parents());
}

var aap;
function firstCommonAncestor(nodeOne, nodeTwo) {
	var nodeOneAncestors = findAncestorsIncludingSelf(nodeOne);
	var nodeTwoAncestors = findAncestorsIncludingSelf(nodeTwo);
	
	cl(nodeOne);
	cl(nodeOneAncestors);
	
	cl(nodeTwo);
	cl(nodeTwoAncestors);
	
	var result = null;

	$(nodeOneAncestors).each(function (index, ancestor) {
		if (jQuery.inArray(ancestor, nodeTwoAncestors) != -1) {
			result = ancestor;
			return false;
		}
	});

	return result;
}

/************* HELPER FUNCTIONS END *****************/

function findPhoneNumberElements() {
	var phoneNumberRegExps = new Array();

	phoneNumberRegExps.push(/\+[1-9][0-9\s()\-\/]{8,25}[0-9]\s/g);
	phoneNumberRegExps.push(/[0-9][0-9\s()\-\/]{6,25}[0-9]\s/g);
	phoneNumberRegExps.push(/[0-9][0-9\s()\-\/]{4,25}[0-9]\s/g);

	var filterFunction = function(possiblePhoneNumber) {
		return (countNrOccurences(possiblePhoneNumber, '-') <= 1 && 
			!possiblePhoneNumber.match(new RegExp(/[0-9]{4}[\s]*\-[\s]*[0-9]{4}$/)) &&
			possiblePhoneNumber.indexOf('\n') == -1 &&
			possiblePhoneNumber.replace(/[\+\s\(\)]*/g, '').length > 5);
	};
	
	pff = filterFunction;

	var result = new Array();
	
	for (var i = 0; i < phoneNumberRegExps.length; i++) {
		result = findElementsWithRegExp(phoneNumberRegExps[i], filterFunction);
		
		if (result.length > 0) {
			break;
		}
	}

	return result;
}
var pff;

function findPostalCodeTownElements() {
	var postalCodeTownRegExps = new Array();

	postalCodeTownRegExps.push(/[^0-9][0-9]{5}[\s]{1,25}[A-Z][a-zA-Z]{1,25}/g);
	postalCodeTownRegExps.push(/[^0-9][0-9]{4}[A-Z]{2}\s/g);
	postalCodeTownRegExps.push(/[^0-9][0-9]{4}\s[A-Z]{2}\s/g);

	var result = new Array();
	
	for (var i = 0; i < postalCodeTownRegExps.length; i++) {
		cl(postalCodeTownRegExps[i]);
		result = findElementsWithRegExp(postalCodeTownRegExps[i]);
		
		if (result.length > 0) {
			break;
		}
	}

	return result;
}

/**
 * filterFunction should return true if element shall be in result set.
 */
function findElementsWithRegExp(regExp, filterFunction) {
	var body = document.getElementsByTagName('body')[0];

	var potentialMatches = body.textContent.match(regExp);
	var result = new Array();
	
	if (potentialMatches == null) {
		return result;
	}

	$(potentialMatches).each(function(index, potentialMatch) {
		potentialMatch = potentialMatch.trim();
		
		cl('pm = ' + potentialMatch);
		cl('re = ' + regExp);

		// select only the closest surrounding element, therefore *last*
		var itemsContainingMatch = getItemsContaining(potentialMatch);

		if (itemsContainingMatch.length == 0) {
			return;
		}
		
		var newMatch = getFirstVisibleElement(itemsContainingMatch);
		
		if (newMatch == null) {
			// No visible element, choose first hidden one
			newMatch = getFirstVisibleElement[0];
		}

		if (filterFunction != undefined && !filterFunction(potentialMatch)) {
			cl('filtered: ' + potentialMatch + '.');
			return;
		}
		
		cl('matched');

		result = $.merge(result, new Array(newMatch));
	});

	return result;
}

function getFirstVisibleElement(array) {
	var result = null;
	
	$(array).each(function (index, element) {
		if (isVisible(element)) {
			result = element;
			return false;
		}
	});
	
	return result;
}

function getItemsContaining(searchString, selector) {
	if (selector == undefined) {
		selector = '*';
	}
	
	return $(selector).filter(function(index, element) {
	    return getTextWithoutChildren(element).indexOf(searchString) != -1;
	});
}

function findDetailedInformation() {
	$('script').remove();
	
	var possibleStartingPointsPhoneNumbers = findPhoneNumberElements();
	var possibleStartingPointsPostalCodeTown = findPostalCodeTownElements();

	if (possibleStartingPointsPhoneNumbers.length == 0 && possibleStartingPointsPostalCodeTown.length == 0) {
		return null;
	}
	
	var startingPoint = null;
	var isCommonAncestor = false;
	
	if (possibleStartingPointsPhoneNumbers.length == 0) {
		startingPoint = possibleStartingPointsPostalCodeTown[0];
		cl('startingPointPostalCodeTown = ');
		cl(startingPoint);
	} else if (possibleStartingPointsPostalCodeTown.length == 0) {
		startingPoint = possibleStartingPointsPhoneNumbers[0];
		cl('startingPointPhoneNumber = ');
		cl(startingPoint);
	} else {
		// Select common ancestor
		var startingPointPhoneNumber = possibleStartingPointsPhoneNumbers[0];
		var startingPointPostalCodeTown = possibleStartingPointsPostalCodeTown[0];
		
		cl('startingPointPhoneNumber = ');
		cl(startingPointPhoneNumber);
		
		cl('startingPointPostalCodeTown = ');
		cl(startingPointPostalCodeTown);

		startingPoint = firstCommonAncestor(startingPointPhoneNumber, startingPointPostalCodeTown);
		isCommonAncestor = true;
	}

	cl('starting point = ');
	cl(startingPoint);

	if (isCommonAncestor) {
		cl('result = ');
		cl(startingPoint);
		cl(startingPoint.textContent);
		
		result = startingPoint;
	} else {
		resultTopStructure = bottomUp(hasSiblingWithSameTopStructure, startingPoint);
		cl('resultTopStructure = ');
		cl(resultTopStructure);
		cl(resultTopStructure.textContent);
		
		result = resultTopStructure;
		
		resultTextContainer = bottomUp(isTextContainer, startingPoint);
		cl('resultTextContainer = ');
		cl(resultTextContainer);
		cl(resultTextContainer.textContent);
	}
	
	return result;
	
//	$(resultTopStructure).css({'background': 'red'});
//	$(resultTextContainer).css({'background': 'green'});
	
	// TODO return the chosen strategy here
}

//function createOverlay() {
//	var element = document.createElement('div');
//
//	getBody().appendChild(element);
//	$(element).css({'z-index': 1000000, 'background' : 'black', 'opacity' : 0.5, 'position' : 'absolute', 'width' : '100%', 'height' : $(getBody()).css('height'), 'top' : '0px'});
//}

function getBody() {
	return document.getElementsByTagName('body')[0];
}

function bottomUp(bottomUpFunction, startNode) {
	var result = startNode;
	
	while (result.textContent.replace(/\s/g, '').length < 20 || bottomUpFunction(result)) {
		result = result.parentNode;
	}
	
	return result;
}

//function bottomUp2(bottomUpFunction, startNode) {
//	var result = startNode;
//	
//	while (result.textContent.replace(/\s/g, '').length < 20 || bottomUpFunction(result.parentNode)) {
//		result = result.parentNode;
//	}
//	
//	return result;
//}

var resultTextContainer;
var resultTopStructure;

var result = findDetailedInformation();