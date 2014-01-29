package org.geotools.data.aggregation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Map.Entry;
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
	private SimpleFeature next = null;
	protected SimpleFeatureBuilder builder;
	private int row;
	private GeometryFactory geometryFactory;
	private ResultSet rs = null;
	private double startX;
	private double grid_deltaX;
	private double startY;
	private double grid_deltaY;
	private long starttime;
	private long grid_delta_time;

	private Object[][] iv_first_obj;
	private int[] range;
	private Map<String, Class> attributes = null;

	private AggregationDataStore data;
	
	SimpleFeatureType areatype = null;

	//    public AggregationFeatureReader(Area area) throws IOException {
	public AggregationFeatureReader(ContentState contentState, ResultSet rs, 
			Object[][] iv_first_obj, int[] range,
			Map<String, Class> attributes ) throws IOException {
		LOGGER.severe("JF: AggregationFeatureReader() created");
		state = contentState;
		data = (AggregationDataStore)contentState.getEntry().getDataStore();
		builder = new SimpleFeatureBuilder(state.getFeatureType());
		// XXXX INCOMPLETE: set prefix or namespace of the result objects here
		geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
		row = 0;
		this.iv_first_obj = iv_first_obj;
		this.range = range;
		this.attributes = attributes;
		this.rs = rs;
		
//		try {
//			areatype = DataUtilities.createType("http://www.nurc.nato.int",
//					"area");
//		} catch (SchemaException e) {
//			System.out.println("SETTING TYPE FAILS:");
//			e.printStackTrace();
//		}
		
		if(rs!=null)
			_init();
	}

	/**
	 * set all parameters derived for constructing the grid at the end 
	 */
	public void _init()throws NullPointerException{
		startX = (Double) iv_first_obj[0][0];
		grid_deltaX = ((Double)iv_first_obj[0][1]) - startX;
		startY = (Double) iv_first_obj[1][0];
		grid_deltaY = ((Double)iv_first_obj[1][1]) - startY;
		if(attributes.containsKey("starttime")){
			starttime = ((Timestamp)iv_first_obj[2][0]).getTime();
			grid_delta_time = ((Timestamp)iv_first_obj[2][1]).getTime()- starttime;
		} else {
			starttime = -1;
			grid_delta_time = 0;
		}
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
		try {
			LOGGER.severe("JF: read next feature start");
			if(rs==null || !rs.next()) return null; // no additional features are available
			LOGGER.severe("JF: FEATURE-READ");
			// long gkey = rs.getLong("gkey");
			boolean hasTime = attributes.containsKey("starttime");
			int pos[] = null;
			if ( hasTime )
				pos = new int[3];
			else 
				pos = new int[2];
			pos[0] = rs.getInt("d0");
			pos[1] = rs.getInt("d1");
			if ( hasTime )
				pos[2] = rs.getInt("d2");
			for(Entry<String,Class> en : attributes.entrySet()){
				String key = en.getKey();
				Class cl = en.getValue();
				Object val = null;
				if(cl == java.lang.Double.class){
					val = rs.getDouble(key);
				} else if(cl == java.lang.Integer.class){
					val = rs.getInt(key);
				} else if(cl == java.lang.Long.class){
					val = rs.getLong(key);
					//				} else if(cl == java.sql.Timestamp.class){
					//					val = rs.getTimestamp(key);
				}	
				builder.set(key, val);
			}

			if(attributes.containsKey("starttime")){
				builder.set("starttime", new Timestamp(starttime+pos[2]*grid_delta_time));
				builder.set("endtime", new Timestamp(starttime+(pos[2]+1)*grid_delta_time));
			}
			// create the ring for the polygon
			Coordinate[] coordinates = new Coordinate[5];
			// lower left corner
			// TODO potentially remove some fraction on the upper bounds of the rectangle 
			// JF: HERE MODIFY RECTANGLE
			double lowX = startX+pos[0]*grid_deltaX;
			double highX = startX+(pos[0]+1)*grid_deltaX;
			double lowY = startY+pos[1]*grid_deltaY;
			double highY = startY+(pos[1]+1)*grid_deltaY;
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
			SimpleFeature res = this.buildFeature();
			if ( areatype != null )
				SimpleFeatureBuilder.retype(res, areatype);
			return res;
		} catch (SQLException e) {
			LOGGER.severe("Serious problem with the database query! "+e.getMessage());
			e.printStackTrace();
			return null;
		}

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
		try {
			if (rs!=null)
				rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		rs = null;
		LOGGER.severe("produced #rows for the output: "+row);
	}

}
