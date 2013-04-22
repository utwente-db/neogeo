if (NeoGeo == undefined) {
	NeoGeo = new Array();
}

if (NeoGeo.AdminPanel == undefined) {
	NeoGeo.AdminPanel = new Array();
}

$(document).ready(function () {
	$('a').click(NeoGeo.AdminPanel.loadResultsOnClick);
	$('menu a').click(NeoGeo.AdminPanel.ProgressBar.reset);
});

NeoGeo.AdminPanel.displayText = function(responseData) {
	var text = responseData.text;
	
	if (responseData.text == undefined || responseData.text == '') {
		text = "Server returned no text";
	}
	
	$('#results').html(text);
	NeoGeo.AdminPanel.ProgressBar.setProgress(1);	
};

NeoGeo.AdminPanel.loadErrorMessage = function(responseData) {
	var text = responseData.responseText;
	
	if (responseData.status != undefined && responseData.status != '200') {
		// Prepend with status code and status text
		text = '<h2>' + responseData.status + ' ' + responseData.statusText + '</h2>' + text;
	}
		
	// Remove style tags and alter h1 tags
	$('#results').append(text
								.replace('<style>', '').replace('</style>', '') // Remove style tags such as Tomcat's
								.replace('h1>', 'h2>'));						// Use smaller headings than the default for error messages

	NeoGeo.AdminPanel.ProgressBar.setFailed();
};

NeoGeo.AdminPanel.updateResults = function(statusURI) {
	NeoGeo.AdminPanel.ProgressBar.reset();
	
	$.getJSON(NeoGeo.AdminPanel.createGetURI(statusURI))
		.success(function(data) {
			var itemToAppendTo = $('#results');
			
			if ($('#results table').size() > 0) {
				itemToAppendTo = $('#results table');
			}
			
			itemToAppendTo.append(data.text);
			NeoGeo.AdminPanel.ProgressBar.setProgress(data.progress);
	
			if (data.progress >= 1) {
				NeoGeo.AdminPanel.periodicalExecutor.stop();
			}
			
			if (data.sourceURL != undefined && data.sourceURL != '') {
				NeoGeo.AdminPanel.lastPage = data.sourceURL;
			}
		})
		.error(function(data) {
			NeoGeo.AdminPanel.loadErrorMessage(data);
		});
};

NeoGeo.AdminPanel.loadResultsOnClick = function(event) {
	$('#results').html('');
	
	if (NeoGeo.AdminPanel.periodicalExecutor != undefined) {
		NeoGeo.AdminPanel.periodicalExecutor.stop();
	}
	
	if (this.href == document.location) {
		return false;
	}
	
	$.getJSON(NeoGeo.AdminPanel.createGetURI(this.href))
		.success(function(data) {
			if (data.statusURI == undefined || data.statusURI == '') {
				NeoGeo.AdminPanel.displayText(data);
			} else {
				NeoGeo.AdminPanel.periodicalExecutor = NeoGeo.PeriodicalExecutor(function() { 
					NeoGeo.AdminPanel.updateResults(data.statusURI); 
				}, 1000);
			}
			
			if (data.sourceURL != undefined && data.sourceURL != '') {
				NeoGeo.AdminPanel.lastPage = data.sourceURL;
			}
			
			// Remove from links with existing handler
			$('a').unbind('click', NeoGeo.AdminPanel.loadResultsOnClick);
			
			// (Re-)add handler to all links (including new ones)
			$('a').click(NeoGeo.AdminPanel.loadResultsOnClick);
		})
		.error(function(data) { 
			NeoGeo.AdminPanel.loadErrorMessage(data);
		});
		
	return false;
};

NeoGeo.AdminPanel.createGetURI = function(uri) {
	var glue = '?';
	
	if (uri.indexOf('?') != -1) {
		glue = '&';
	}
	
	var result = uri + glue + 'sourceURI=' + NeoGeo.AdminPanel.lastPage;
	
	NeoGeo.AdminPanel.lastPage = uri;
	
	return result;
};