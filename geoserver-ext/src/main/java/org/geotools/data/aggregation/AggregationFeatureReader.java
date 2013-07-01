package org.geotools.data.aggregation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Timestamp;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.store.ContentState;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;

// Referenced classes of package org.geotools.data.aggregation:
//            AggregationDataStore

public class AggregationFeatureReader implements FeatureReader {
	private static final Logger LOGGER = Logger.getLogger("org.geotools.data.aggregation.AggregationFeatureReader");

    protected ContentState state;
    protected Connection con;
    private SimpleFeature next;
    protected SimpleFeatureBuilder builder;
    private int row;
    private GeometryFactory geometryFactory;
    private int grid_row;
    private int grid_column;
    private double grid_deltaX;
    private double grid_deltaY;
private AggregationDataStore data;
	private int xSize;

	private int ySize;

	private double startX;

	private double startY;
	// TODO this has to be retrieved later on from the PreAggregate object 
    public static final double	DFLT_BASEBOXSIZE = 0.001;
	
//    public AggregationFeatureReader(Area area) throws IOException {
        public AggregationFeatureReader(ContentState contentState, Area area) throws IOException {
        state = contentState;
        data = (AggregationDataStore)contentState.getEntry().getDataStore();
        builder = new SimpleFeatureBuilder(state.getFeatureType());
        geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
        row = 0;
        xSize = data.getXSize();
        ySize = data.getYSize();
        // number of basic units to be split up in boxes in the map
        double origDiffX = (area.getHighX()-area.getLowX())/DFLT_BASEBOXSIZE;
        // the goal is to have xSize boxes thus have a look how many baseboxsizes 
        // can be used per box
        grid_deltaX = Math.round(origDiffX/((double) xSize))*DFLT_BASEBOXSIZE;
        
        // determine the start of the first box 
        startX = Math.ceil(area.getLowX()/DFLT_BASEBOXSIZE)* DFLT_BASEBOXSIZE;
        // determine the number of boxes which can be displayed
        double endX = Math.floor(area.getHighX()/DFLT_BASEBOXSIZE)* DFLT_BASEBOXSIZE;
        xSize = grid_deltaX==0 ? 0 : (int) Math.floor((endX - startX)/grid_deltaX);
        
        double origDiffY = (area.getHighY()-area.getLowY())/DFLT_BASEBOXSIZE;
        grid_deltaY = Math.round(origDiffY/((double) ySize))*DFLT_BASEBOXSIZE;
        // determine the start of the first box 
        startY = Math.ceil(area.getLowY()/DFLT_BASEBOXSIZE)* DFLT_BASEBOXSIZE;
        // determine the number of boxes which can be displayed
        double endY = Math.floor(area.getHighY()/DFLT_BASEBOXSIZE)* DFLT_BASEBOXSIZE;
        ySize = grid_deltaY==0 ? 0 : (int) Math.floor((endY - startY)/grid_deltaY);
        
        //TODO check with Jan whether this is indeed the way the intervals are calculated
        
        LOGGER.severe("area: "+area.toString());
        LOGGER.severe("X: startX:"+startX+"   xSize:"+xSize+"    grid_deltaX:"+grid_deltaX);
        LOGGER.severe("Y: startY:"+startY+"   ySize:"+ySize+"    grid_deltaY:"+grid_deltaY);
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
         if(row==xSize*ySize) return null; // no additional features are available
         
        // TODO change to retrieve the right data from the partial result
        // TODO check that the construction of the polygon equals the column and row index of 
        // the grid in the pre aggregate query result   
        if(data.hasOutputCount())
			builder.set("cnt", 10);
		if(data.hasOutputSum())
			builder.set("sum", 20);
		if(data.hasOutputMin())
			builder.set("min", 11);
		if(data.hasOutputMax())
			builder.set("max", 19);
                
		builder.set("time", new Timestamp(2002,10,01,23,59,59,999));
		// create the ring for the polygon
		Coordinate[] coordinates = new Coordinate[5];
        // lower left corner
		// TODO potentially remove some fraction on the upper bounds of the rectangle 
		double lowX = startX+grid_column*grid_deltaX;
		double highX = startX+(grid_column+1)*grid_deltaX;
		double lowY = startY+grid_row*grid_deltaY;
		double highY = startY+(grid_row+1)*grid_deltaY;
		// low left corner
		coordinates[0] = new Coordinate(lowX,lowY);
		// high left corner
		coordinates[1] = new Coordinate(lowX,highY);
		// high right corner
		coordinates[2] = new Coordinate(highX,highY);
		// low right corner 
		coordinates[3] = new Coordinate(highX,lowY);
		// low left corner
		coordinates[4] = coordinates[0];
		
         LinearRing lr = geometryFactory.createLinearRing(coordinates);
         builder.set("area", geometryFactory.createPolygon(lr, null) );
         grid_column++;
         if(grid_column>=xSize){
        	 grid_row++;
        	 grid_column=0;
         }
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
