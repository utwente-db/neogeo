NeoGeo.PeriodicalExecutors = new Array();

NeoGeo.PeriodicalExecutor = function(functionToExecute, msPeriod) {
    //This section is similar to the initialize() method from prototypejs.
	this.functionToExecute = functionToExecute;
	this.msPeriod = msPeriod;
	this.stopped = false;
	
	this.id = NeoGeo.PeriodicalExecutors.length;
	NeoGeo.PeriodicalExecutors[this.id] = this;
	
    //Adding a method to an object
    this.execute = function () {
    	if (this.stopped) {
			return;
		}
    	
		this.functionToExecute();
		window.setTimeout("NeoGeo.PeriodicalExecutors[" + this.id + "].execute()", this.msPeriod);
    };
    
    this.stop = function() {
		this.stopped = true;
    };

	this.execute();
	
	return NeoGeo.PeriodicalExecutors[this.id];
};