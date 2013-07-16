var map;



function init() {
	format = 'image/png';
	// creates an event handler for double clicks
//	OpenLayers.Control.Click = OpenLayers.Class(OpenLayers.Control, {                
//		defaultHandlerOptions: {
//		'single': false,
//		'double': true,
//		'pixelTolerance': 0,
//		'stopSingle': false,
//		'stopDouble': true
//	},
//
//	initialize: function(options) {
//		this.handlerOptions = OpenLayers.Util.extend(
//				{}, this.defaultHandlerOptions
//		);
//		OpenLayers.Control.prototype.initialize.apply(
//				this, arguments
//		); 
//		this.handler = new OpenLayers.Handler.Click(
//				this, {
//					'dblclick': this.trigger
//				}, this.handlerOptions
//		);
//	}, 
//
//	trigger: function(e) {
//		var lonlat = map.getLonLatFromPixel(e.xy);
//		alert("You clicked near " + lonlat.lat + " N, " +
//				+ lonlat.lon + " E");
//		
//	}
//
//	});
//	var nav = new OpenLayers.Control.Navigation({
//	  defaultDblClick: function(event) { alert("navigation"); }
//  });
//	var zoom = //Here it come
//      new OpenLayers.Control.ZoomBox({alwaysZoom:true
////    	  defaultDblClick: function(event) { alert("navigation");
//    	  ,zoomOnClick:false
//    	  });
	

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
		layers : [],
		sf : null,
	
		initialize: function(options) {
		var panel = new OpenLayers.Control.Panel({displayClass: 'panel', allowDepress: false});
		this.sf = new OpenLayers.Control.SelectFeature([]);
		var zoomBox = new OpenLayers.Control.ZoomBox();
		var navigation = new OpenLayers.Control.Navigation();

		var featureSelectBtn = new OpenLayers.Control.Button({displayClass: 'olControlSelectFeature', type: OpenLayers.Control.TYPE_TOOL,
		    eventListeners: {
		       'activate': function(){this.sf.activate(); zoomBox.deactivate(); navigation.deactivate();}, 
		       'deactivate': function(){this.sf.deactivate()}
		    }
		});

		var zoomBoxBtn = new OpenLayers.Control.Button({displayClass: 'olControlZoomBox', type: OpenLayers.Control.TYPE_TOOL,
		    eventListeners: {
		       'activate': function(){zoomBox.activate(); navigation.deactivate(); this.sf.deactivate()}, 
		       'deactivate': function(){zoomBox.deactivate()}
		    }
		});

		var navigationBtn = new OpenLayers.Control.Button({displayClass: 'olControlNavigation', type: OpenLayers.Control.TYPE_TOOL,
		    eventListeners: {
		       'activate': function(){navigation.activate(); this.sf.deactivate(); zoomBox.deactivate();}, 
		       'deactivate': function(){navigation.deactivate()}
		    }
		});
		        
		panel.addControls([featureSelectBtn, zoomBoxBtn, navigationBtn]);

		OpenLayers.Control.Panel.prototype.initialize.apply(this, [options]);
		this.addControls([navigation,zoomBox,this.sf]);
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
	},
	
	setLayer: function(layers_new) {
		this.layers = layers_new;
		//this.sf.setLayer(this.layers);
	}
	});
	
	var options = {
			controls: [],
			projection: "EPSG:3857",
			units: 'm',
			center: new OpenLayers.LonLat(-0.1, 51)
	// Google.v3 uses web mercator as projection, so we have to
	// 	transform our coordinates
	.transform('EPSG:4326', 'EPSG:3857'),
	zoom: 5
	};


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
		         //center: new OpenLayers.LonLat(-0.1, 51)
		         // Google.v3 uses web mercator as projection, so we have to
		         // transform our coordinates
		         //    .transform('EPSG:4326', 'EPSG:3857'),
		         //zoom: 5
		         units: 'm',
		         center: new OpenLayers.LonLat(-0.1, 51)
	// Google.v3 uses web mercator as projection, so we have to
	// 	transform our coordinates
	.transform('EPSG:4326', 'EPSG:3857'),
	zoom: 5
	});
//	var click = new OpenLayers.Control.Click();
//    map.addControl(click);
//    click.activate();
//	featureSelectBtn.activate();
	
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

	var panel = new OpenLayers.Control.CustomNavToolbar();
	panel.setLayer(neogeo_uk_agg.params.LAYERS);
//	map.addControl(new OpenLayers.Control.PanZoomBar({
//	position: new OpenLayers.Pixel(2, 15)
//	}));
	map.addControl(new OpenLayers.Control.Permalink());
	map.addControl(new OpenLayers.Control.ScaleLine());
	map.addControl(new OpenLayers.Control.Permalink('permalink'));
	map.addControl(panel);
	map.addControl(new OpenLayers.Control.LayerSwitcher());
	map.addControl(new OpenLayers.Control.MousePosition({element: $('location')}));

    
	// support GetFeatureInfo
	// support GetFeatureInfo
	map.events.register('click', map, function (e) {
		if(panel.sf.active){
	document.getElementById('nodelist').innerHTML = "Loading... please wait...";
	var params = {
	REQUEST: "GetFeatureInfo",
	EXCEPTIONS: "application/vnd.ogc.se_xml",
	BBOX: map.getExtent().toBBOX(),
	SERVICE: "WMS",
	INFO_FORMAT: 'text/html',
	QUERY_LAYERS: panel.layers,
	FEATURE_COUNT: 50,
	Layers: 'nurc:london_hav_neogeo',
	WIDTH: map.size.w,
	HEIGHT: map.size.h,
	format: format,
	styles: '',
	srs: 'EPSG:3857'
	};
	// handle the wms 1.3 vs wms 1.1 madness
	//if(map.layers[0].params.VERSION == "1.3.0") {
//	params.version = "1.3.0";
//	params.j = parseInt(e.xy.x);
//	params.i = parseInt(e.xy.y);
	//} else {
	params.version = "1.1.1";
	params.x = parseInt(e.xy.x);
	params.y = parseInt(e.xy.y);
//	}
	// merge filters
	OpenLayers.loadURL("http://silo2.ewi.utwente.nl:8080/geoserver/nurc/wms", params, this, setHTML, setHTML);
	OpenLayers.Event.stop(e);
		}
	});
	//}

//sets the HTML provided into the nodelist element
function setHTML(response){
	document.getElementById('nodelist').innerHTML = response.responseText;
};
}

