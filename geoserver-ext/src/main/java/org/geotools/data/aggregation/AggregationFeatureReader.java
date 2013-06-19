package org.geotools.data.aggregation;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import java.io.IOException;
import java.sql.Connection;
import java.util.NoSuchElementException;
import org.geotools.data.FeatureReader;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;

// Referenced classes of package org.geotools.data.aggregation:
//            AggregationDataStore

public class AggregationFeatureReader implements FeatureReader {
	
    protected ContentState state;
    protected Connection con;
    private SimpleFeature next;
    protected SimpleFeatureBuilder builder;
    private int row;
    private GeometryFactory geometryFactory;
    private Area area;

//    public AggregationFeatureReader(Area area) throws IOException {
        public AggregationFeatureReader(ContentState contentState, Area area) throws IOException {
        state = contentState;
        AggregationDataStore agg = (AggregationDataStore)contentState.getEntry().getDataStore();
        builder = new SimpleFeatureBuilder(state.getFeatureType());
        geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
        row = 0;
        this.area = area;
        
    }

    public SimpleFeatureType getFeatureType() {
    	return state.getFeatureType();
    }

    public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
        SimpleFeature feature;
        if(next != null) {
            feature = next;
            next = null;
        } else {
            feature = readFeature();
        }
        return feature;
    }

    SimpleFeature readFeature() throws IOException {
    	 if( reader == null ){
             throw new IOException("FeatureReader is closed; no additional features can be read");
         }
         boolean read = reader.readRecord(); // read the "next" record
         if( read == false ){
             close(); // automatic close to be nice
             return null; // no additional features are available
         }
         Coordinate coordinate = new Coordinate();
         for( String column : reader.getHeaders() ){
             String value = reader.get(column);
             if( "lat".equalsIgnoreCase(column)){
                 coordinate.y = Double.valueOf( value.trim() );
             }
             else if( "lon".equalsIgnoreCase(column)){
                 coordinate.x = Double.valueOf( value.trim() );
             }
             else {
                 builder.set(column, value );
             }
         }
         builder.set("Location", geometryFactory.createPoint( coordinate ) );
         
         return this.buildFeature();
    }

    protected SimpleFeature buildFeature() {
        row++;
        return builder.buildFeature((new StringBuilder()).append(state.getEntry().getTypeName()).append(".").append(row).toString());
    }

    public boolean hasNext() throws IOException {
        if(next != null) {
            return true;
        } else {
            next = readFeature();
            return next != null;
        }
    }

    public void close() throws IOException {
        builder = null;
        geometryFactory = null;
        next = null;
    }

}
