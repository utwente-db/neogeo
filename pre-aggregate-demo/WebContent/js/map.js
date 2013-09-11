var map;



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

	neogeo_uk = new OpenLayers.Layer.WMS(
			"tweets UK", "http://silo3.ewi.utwente.nl:9090/geoserver/nurc/wms",
			{
				layers: 'nurc:uk_neogeo',
				maxfeatures: 100,
				format: 'image/gif',
				transparent: 'true'
					//time: '2002-09-01T00:00:00.0Z/2002-10-01T23:59:59.999Z',
					//styleMap: new OpenLayers.StyleMap(style)
			},{
				opacity: 0.5
			}
	);
	
//	var renderer = OpenLayers.Layer.Vector.prototype.renderers;
    
//	neogeo_uk = new OpenLayers.Layer.Vector("twitter_WFS", {
//		strategies : [new OpenLayers.Strategy.BBOX()],
//		protocol : new OpenLayers.Protocol.WFS({
//			version : "1.1.0",
//			url : "http://silo3.ewi.utwente.nl:9090/geoserver/wfs", 
//			featurePrefix : "nurc:",
//			featureType : "uk_neogeo",
//			featureNS : "", 
//			geometryName : "coordinates", //type: "Geometry"
//			srsName : "EPSG:4326"
//		}),
//		renderers : renderer
//	});
	
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
			"tweet aggregate UK", "http://silo3.ewi.utwente.nl:9090/geoserver/nurc/wms",
			{
				layers: 'nurc:aggregate_uk_neogeo___myAggregate',
				format: 'image/gif',
				transparent: 'true'
					//time: '2002-09-01T00:00:00.0Z/2002-10-01T23:59:59.999Z',
					//styleMap: new OpenLayers.StyleMap(style)
			}, {
				singleTile: true,
				ratio: 1,
				opacity: 0.5
			}
	);
	map.addLayers([neogeo_uk_agg, neogeo_uk]);
//	map.addLayers([neogeo_uk]);
	//neogeo_uk.display(false);

	var panel = new OpenLayers.Control.CustomNavToolbar();
	panel.setLayer([neogeo_uk_agg.params.LAYERS, neogeo_uk.params.LAYERS]);
	
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
					QUERY_LAYERS: neogeo_uk.params.LAYERS,
					FEATURE_COUNT: 50,
					Layers: 'nurc:uk_neogeo',
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
			// merge filters
			OpenLayers.loadURL("http://silo3.ewi.utwente.nl:9090/geoserver/nurc/wms", params, this, setHTML, setHTML);
			OpenLayers.Event.stop(e);
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
//	neogeo_uk_agg.mergeNewParams({'time':startstring+'/'+endstring});
	neogeo_uk.mergeNewParams({'time':startstring+'/'+endstring});
}

function update_viewparams() {
	var viewparams = "keyword1:"+OpenLayers.Util.getElement('keyword1').value + 
	";keyword2:"+OpenLayers.Util.getElement('keyword2').value + 
	";keyword3:"+OpenLayers.Util.getElement('keyword3').value + 
	";keyword4:"+OpenLayers.Util.getElement('keyword4').value + 
	";keyword5:"+OpenLayers.Util.getElement('keyword5').value;
	var query = OpenLayers.Util.getElement('query');
	if(query.checked){
		viewparams = viewparams+";query:standard";
	}
//	neogeo_uk_agg.mergeNewParams({'viewparams':viewparams});
	neogeo_uk.mergeNewParams({'viewparams':viewparams});
}
