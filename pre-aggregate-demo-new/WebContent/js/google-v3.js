var map;

function init() {
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
    map.addControl(new OpenLayers.Control.LayerSwitcher());
    neogeo_uk_agg = new OpenLayers.Layer.WMS(
            "tweets UK", "http://silo3.ewi.utwente.nl:9090/geoserver/nurc/wms",
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
