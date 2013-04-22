if (NeoGeo == undefined) {
	NeoGeo = new Array();
}

if (NeoGeo.AdminPanel == undefined) {
	NeoGeo.AdminPanel = new Array();
}

if (NeoGeo.AdminPanel.ProgressBar == undefined) {
	NeoGeo.AdminPanel.ProgressBar = new Array();
}

/* Progress bar - START */
NeoGeo.AdminPanel.ProgressBar.set = function(color, fraction) {
	// Progress green and empty
	$('#progressBarCompleted').css('background-color', color);
	$('#progressBarCompleted').css('width', (fraction * 100) + '%');
};

NeoGeo.AdminPanel.ProgressBar.setProgress = function(fraction) {
	NeoGeo.AdminPanel.ProgressBar.set('green', fraction);
};

NeoGeo.AdminPanel.ProgressBar.setFailed = function() {
	NeoGeo.AdminPanel.ProgressBar.set('red', 1.0);
};

NeoGeo.AdminPanel.ProgressBar.reset = function() {
	NeoGeo.AdminPanel.ProgressBar.setProgress(0);
};
/* Progress bar - END */