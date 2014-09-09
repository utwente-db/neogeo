var map;

if (typeof String.prototype.endsWith !== 'function') {
    String.prototype.endsWith = function(suffix) {
        return this.indexOf(suffix, this.length - suffix.length) !== -1;
    };
}

function init() {
	format = 'image/png';
	// creates an event handler for double clicks
//	OpenLayers.Control.Click = OpenLayers.Class(OpenLayers.Control, {                
//	defaultHandlerOptions: {
//	'single': false,
//	'double': true,
//	'pixelTolerance': 0,
//	'stopSingle': false,
//	'stopDouble': true
//	},

//	initialize: function(options) {
//	this.handlerOptions = OpenLayers.Util.extend(
//	{}, this.defaultHandlerOptions
//	);
//	OpenLayers.Control.prototype.initialize.apply(
//	this, arguments
//	); 
//	this.handler = new OpenLayers.Handler.Click(
//	this, {
//	'dblclick': this.trigger
//	}, this.handlerOptions
//	);
//	}, 

//	trigger: function(e) {
//	var lonlat = map.getLonLatFromPixel(e.xy);
//	alert("You clicked near " + lonlat.lat + " N, " +
//	+ lonlat.lon + " E");

//	}

//	});
//	var nav = new OpenLayers.Control.Navigation({
//	defaultDblClick: function(event) { alert("navigation"); }
//	});
//	var zoom = //Here it come
//	new OpenLayers.Control.ZoomBox({alwaysZoom:true
////	defaultDblClick: function(event) { alert("navigation");
//	,zoomOnClick:false
//	});


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



	var mapOptions = {
		controls: [],
		projection: "EPSG:3857",
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
		units: 'm'
	};

	if (typeof(MAP_ZOOM) != 'undefined') {
		mapOptions.zoom = MAP_ZOOM;
	} else {
		mapOptions.zoom = 5;
	}

	if (typeof(MAP_CENTER) != 'undefined') {
		mapOptions.center = MAP_CENTER;
	} else {
		mapOptions.center = new OpenLayers.LonLat(-0.1, 51).transform('EPSG:4326', 'EPSG:3857');
	}


	// definition of the map
	map = new OpenLayers.Map('map', mapOptions);


	var neogeo_uk = null;

	if (typeof(TWEETS_URL) != 'undefined' && TWEETS_URL != false) {
		if (TWEETS_URL.toLowerCase().endsWith('wfs')) {
			// setup tweets layer as WFS layer
			var renderer = OpenLayers.Layer.Vector.prototype.renderers;

			var split = TWEETS_LAYER.split(':');
			var prefix = split[0];
			var type = split[1];

			var layerOptions = {
				strategies : [new OpenLayers.Strategy.BBOX({ratio:1})],
				protocol : new OpenLayers.Protocol.WFS({
					version : "1.1.0",
					url : TWEETS_URL, 
					featurePrefix : prefix,
					featureType : type,
					featureNS : "", 
					geometryName : "coordinates", //type: "Geometry"
					srsName : "EPSG:3857",
					maxFeatures : 500
				}),
				styleMap: new OpenLayers.StyleMap({
					graphicName: "square",
					pointRadius: 2,
					fillColor: "#FF5B29",
					fillOpacity: 0.7,
					strokeColor: "#FF5B29"			
				}),
				renderers : renderer
			};

			if (typeof(TWEETS_MIN_SCALE) != 'undefined' && TWEETS_MIN_SCALE != false) {
				layerOptions.minScale = TWEETS_MIN_SCALE;
			}
    
			neogeo_uk = new OpenLayers.Layer.Vector(TWEETS_TITLE, layerOptions);
		} else if (TWEETS_URL.toLowerCase().endsWith('wms')) {
			tweets_options = {opacity: 0.5};

			// setup tweets layer as WMS layer
			if (typeof(TWEETS_MIN_SCALE) != 'undefined' && TWEETS_MIN_SCALE != false) {
				tweets_options.minScale = TWEETS_MIN_SCALE;
			}

			neogeo_uk = new OpenLayers.Layer.WMS(
					TWEETS_TITLE, 
					TWEETS_URL,
					{
						layers: TWEETS_LAYER,
						maxfeatures: 100,
						format: 'image/gif',
						transparent: 'true'
					},
					tweets_options
			);
		} else {
			console.error("URL of Tweets layer is invalid; must end in 'wfs' or 'wms'");
		}
	}
		
	
	
//	neogeo_uk = new OpenLayers.Layer.WFS("wfs testdata",
//			"http://silo3.ewi.utwente.nl:9090/geoserver/wfs",
//			{
//			"TYPENAME": "nurc:uk_neogeo",
//			"SERVICE": "WFS",
//			"VERSION": "1.0.0",
//			"REQUEST": "GetFeature",
//			"SRS": "EPSG:4326"
//			},
//			{
//			"typename": "nurc:uk_neogeo",
//			"maxfeatures": 1000,
//			"extractAttributes": true,
//			"isBaseLayer": false,
//			"isVisible": false,
//			"buffer": 1,
//			"opacity": 1,
//			"displayOutsideMaxExtent": true
//			});
//	var style_green = {
//			strokeColor: "#00FF00",
//			strokeWidth: 2,
//			fillColor: "#00FF00",
//			fillOpacity: 0.25,
//			strokeDashstyle: "dashdot",
//			pointRadius: 6,
//			pointerEvents: "visiblePainted"
//			}; 
//	neogeo_uk.styleMap = new OpenLayers.StyleMap(new OpenLayers.Style(style_green)); 

	neogeo_uk_agg = new OpenLayers.Layer.WMS(
			AGGREGATE_TITLE, 
			AGGREGATE_URL,
			{
				layers: AGGREGATE_LAYER,
				format: 'image/gif',
				transparent: 'true',
					//time: '2002-09-01T00:00:00.0Z/2002-10-01T23:59:59.999Z',
					//styleMap: new OpenLayers.StyleMap(style)
				viewparams: (typeof(AGGREGATE_VIEW_PARAMS) != 'undefined') ? AGGREGATE_VIEW_PARAMS : ''
			}, {
				singleTile: true,
				ratio: 1,
				opacity: 0.5
			}
	);

	var layers = [];
	var panelLayers = [];

	if (AGGREGATE_URL != false && AGGREGATE_LAYER != false) {
		layers[layers.length] = neogeo_uk_agg;
		panelLayers[panelLayers.length] = AGGREGATE_LAYER;
	}

	if (TWEETS_URL != false && TWEETS_LAYER != false) {
		layers[layers.length] = neogeo_uk;
		panelLayers[panelLayers.length] = TWEETS_LAYER;
	}

	map.addLayers(layers);

	var panel = new OpenLayers.Control.CustomNavToolbar();
	panel.setLayer(panelLayers);
	
	//map.addControl(new OpenLayers.Control.PanZoomBar({
	//position: new OpenLayers.Pixel(2, 15)
	//}));
	map.addControl(new OpenLayers.Control.Zoom());
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
					QUERY_LAYERS: TWEETS_LAYER,
					FEATURE_COUNT: 50,
					Layers: TWEETS_LAYER,
					WIDTH: map.size.w,
					HEIGHT: map.size.h,
					format: format,
					styles: '',
					srs: 'EPSG:3857'
			};
			// handle the wms 1.3 vs wms 1.1 madness
			//if(map.layers[0].params.VERSION == "1.3.0") {
//			params.version = "1.3.0";
//			params.j = parseInt(e.xy.x);
//			params.i = parseInt(e.xy.y);
			//} else {
			params.version = "1.1.1";
			params.x = parseInt(e.xy.x);
			params.y = parseInt(e.xy.y);
//			}

			var url = TWEETS_URL;
			if (url.toLowerCase().endsWith('wfs')) {
				url = url.substring(0, url.length - 3) + 'wms';
			}

			// merge filters
			OpenLayers.loadURL(url, params, this, setHTML, setHTML);
			OpenLayers.Event.stop(e);
		}
	});


	map.events.register('moveend', map, function (e) {
		// only a WFS service needs to be manually refreshed
		if (typeof(TWEETS_URL) != 'undefined' && TWEETS_URL != false && TWEETS_URL.toLowerCase().endsWith('wfs')) {
			neogeo_uk.refresh({force: true}); 
		}
	});


	//}

//	sets the HTML provided into the nodelist element
	function setHTML(response){
		document.getElementById('nodelist').innerHTML = response.responseText;
	};
}

function update_date() {
	var startstring = OpenLayers.Util.getElement('startyear').value + "-" +
	OpenLayers.Util.getElement('startmonth').value + "-" +
	OpenLayers.Util.getElement('startday').value + "T" +
	OpenLayers.Util.getElement('starthour').value + ":" +
	OpenLayers.Util.getElement('startminute').value + ":00.0Z";
	var endstring = OpenLayers.Util.getElement('endyear').value + "-" +
	OpenLayers.Util.getElement('endmonth').value + "-" +
	OpenLayers.Util.getElement('endday').value + "T" +
	OpenLayers.Util.getElement('endhour').value + ":" +
	OpenLayers.Util.getElement('endminute').value + ":00.0Z";

	if (typeof(neogeo_uk_agg) != 'undefined') {
		neogeo_uk_agg.mergeNewParams({'time':startstring+'/'+endstring});
	}

	if (typeof(neogeo_uk) != 'undefined') {
		neogeo_uk.mergeNewParams({'time':startstring+'/'+endstring});
	}
}

function update_viewparams(viewparams) {
	if (typeof(viewparams) == 'undefined') {
		viewparams = "";
	}

	for(var i=1; i <= 5; i++) {
		var field = OpenLayers.Util.getElement('keyword' + i);

		if (field != null) {
			viewparams += ";keyword" + i + ":" + field.value;
		}
	}

	var query = OpenLayers.Util.getElement('query');
	if(query != null && query.checked){
		viewparams = viewparams+";query:standard";
	}

	if (typeof(neogeo_uk_agg) != 'undefined') {
		neogeo_uk_agg.mergeNewParams({'viewparams':viewparams});
	}

	if (typeof(neogeo_uk) != 'undefined') {
		neogeo_uk.mergeNewParams({'viewparams':viewparams});
	}
}
