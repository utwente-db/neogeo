var map;



function init() {
	//Creation of a custom panel with a ZoomBox control with the alwaysZoom option sets to true				
	OpenLayers.Control.CustomNavToolbar = OpenLayers.Class(OpenLayers.Control.Panel, {

	    /**
	     * Constructor: OpenLayers.Control.NavToolbar 
	     * Add our two mousedefaults controls.
	     *
	     * Parameters:
	     * options - {Object} An optional object whose properties will be used
	     *     to extend the control.
	     */
		
		
	    initialize: function(options) {
	        OpenLayers.Control.Panel.prototype.initialize.apply(this, [options]);
	        this.addControls([
	          new OpenLayers.Control.Navigation(),
			  //Here it come
	          new OpenLayers.Control.GetFeature(),
	          new OpenLayers.Control.ZoomBox({alwaysZoom:true})
	        ]);
			// To make the custom navtoolbar use the regular navtoolbar style
			this.displayClass = 'olControlNavToolbar'
	    },
		
		
	
	    /**
	     * Method: draw 
	     * calls the default draw, and then activates mouse defaults.
	     */
	    draw: function() {
	        var div = OpenLayers.Control.Panel.prototype.draw.apply(this, arguments);
            this.defaultControl = this.controls[0];
	        return div;
	    }
	});

	// definition of the map
	map = new OpenLayers.Map('map', {
        projection: 'EPSG:3857',
        layers: [
            new OpenLayers.Layer.Google(
                "Google Physical",
                {type: google.maps.MapTypeId.TERRAIN}
            ),
            new OpenLayers.Layer.Google(
                "Google Streets", // the default
                {numZoomLevels: 20}
            ),
            new OpenLayers.Layer.Google(
                "Google Hybrid",
                {type: google.maps.MapTypeId.HYBRID, numZoomLevels: 20}
            ),
            new OpenLayers.Layer.Google(
                "Google Satellite",
                {type: google.maps.MapTypeId.SATELLITE, numZoomLevels: 22}
            )
        ],
        center: new OpenLayers.LonLat(-0.1, 51)
            // Google.v3 uses web mercator as projection, so we have to
            // transform our coordinates
            .transform('EPSG:4326', 'EPSG:3857'),
        zoom: 5
    });
    var panel = new OpenLayers.Control.CustomNavToolbar();
	
    map.addControl(new OpenLayers.Control.Permalink());
    map.addControl(new OpenLayers.Control.ScaleLine());
    map.addControl(new OpenLayers.Control.Permalink('permalink'));
    map.addControl(new OpenLayers.Control.MousePosition());
    map.addControl(panel);
    map.addControl(new OpenLayers.Control.LayerSwitcher());
    
    neogeo_uk_agg = new OpenLayers.Layer.WMS(
            "tweets UK", "http://silo2.ewi.utwente.nl:8080/geoserver/nurc/wms",
            {
                layers: 'nurc:london_hav_neogeo',
                format: 'image/gif',
                transparent: 'true'
        		//time: '2002-09-01T00:00:00.0Z/2002-10-01T23:59:59.999Z',
                //styleMap: new OpenLayers.StyleMap(style)
            }
            );
    map.addLayers([neogeo_uk_agg]);
    
    // add behavior to html
    var animate = document.getElementById("animate");
    animate.onclick = function() {
        for (var i=map.layers.length-1; i>=0; --i) {
            map.layers[i].animationEnabled = this.checked;
        }
    };
}
