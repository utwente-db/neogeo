package org.geotools.data.aggregation;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.geotools.data.FeatureReader;
import org.geotools.data.Join;
import org.geotools.data.Query;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureCollection;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Polygon;

@SuppressWarnings("unchecked")
/**
 * 
 *
 * @source $URL$
 */
public class AggregationFeatureSource extends ContentFeatureSource {
	private static final Logger LOGGER = Logger.getLogger("org.geotools.data.aggregation.AggregationFeatureSource");

	private int crsNumber = -1;
	private int totalCnt = -1;
	private Object totalBounds = null;

	public AggregationFeatureSource(ContentEntry entry, Query query) {
		super(entry,query);
	}
	/**
	 * Access parent AggregationDataStore
	 */
	public AggregationDataStore getDataStore(){
		return (AggregationDataStore) super.getDataStore();
	}

	public void test(){
		SimpleFeatureType s = this.getSchema();
		CoordinateReferenceSystem crs = s.getCoordinateReferenceSystem();
		crs.getCoordinateSystem();
	}

	/**
	 * Implementation that generates the total bounds
	 * (many file formats record this information in the header)
	 */
	protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
		LOGGER.severe("filter: "+query.getFilter().getClass().getCanonicalName());
		CoordinateReferenceSystem crs = query.getCoordinateSystem();
		if (crs!=null) LOGGER.severe("query CRS: "+crs.getAlias());

		
		ReferencedEnvelope bounds = this.getDataStore().getReferencedEnvelope(entry, getSchema().getCoordinateReferenceSystem());
//		double x1;
//		double x2;
//		double y1;
//		double y2;
		// parameters double x1, double x2, double y1, double y2, CoordinateReferenceSystem crs
		//ReferencedEnvelope bounds = new ReferencedEnvelope(x1,x2,y1,y2, getSchema().getCoordinateReferenceSystem() );
		
		//		DirectPosition lc = bounds.getLowerCorner();
		//		DirectPosition uc = bounds.getUpperCorner();
		//		double[] lcord = lc.getCoordinate();
		//		double[] ucord = uc.getCoordinate();
		//		LOGGER.severe("extracting the bounding box of the request:");
		//		LOGGER.severe("lower corner dimensions : "+lcord.length);
		//		LOGGER.severe("lower corner 0 : "+lcord[0]);
		//		LOGGER.severe("lower corner 1 : "+lcord[1]);
		//		LOGGER.severe("upper corner dimensions : "+ucord.length);
		//		LOGGER.severe("upper corner 0 : "+ucord[0]);
		//		LOGGER.severe("upper corner 1 : "+ucord[1]);
//		FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = getReaderInternal(query);
//		try {
//			while( featureReader.hasNext() ){
//				SimpleFeature feature = featureReader.next();
//				bounds.include( feature.getBounds() );
//			}
//		}
//		finally {
//			featureReader.close();
//		}
		return bounds;
	}

	protected int getCountInternal(Query query) throws IOException {
		int count = -1;
		if (query.getFilter() == Filter.INCLUDE) {
			if(totalCnt==-1) 
				totalCnt = this.getDataStore().getTotalCount(query.getTypeName());
			count = totalCnt;
		}else {
			ContentFeatureCollection cfc = this.getFeatures(query);
			count = 0;
			SimpleFeatureIterator iter = cfc.features();
			while(iter.hasNext()) {
				iter.next();
				count++;
			}
			iter.close();
		}
		return count;
	}

	protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query)
	throws IOException {
		LOGGER.severe("query properties: "+query.getProperties());
		// Note we ignore 'query' because querying/filtering is handled in superclasses.
		// http://opensourcejavaphp.net/java/geotools/org/geotools/data/store/ContentFeatureSource.java.html
		// set capabilities to avoid checking by superclass
		// canFilter() 
		// canReproject()
		// canOffset()
		// canLimit()
		// canSort()
		// canRetype() 
		ReferencedEnvelope bounds = new ReferencedEnvelope(query.getCoordinateSystem());
//		DirectPosition lc = bounds.getLowerCorner();
//		DirectPosition uc = bounds.getUpperCorner();
//		double[] lcord = lc.getCoordinate();
//		double[] ucord = uc.getCoordinate();
		LOGGER.severe("extracting the bounding box of the request:");
//		LOGGER.severe("lower corner dimensions : "+lcord.length);
//		LOGGER.severe("lower corner 0 : "+lcord[0]);
//		LOGGER.severe("lower corner 1 : "+lcord[1]);
//		LOGGER.severe("upper corner dimensions : "+ucord.length);
//		LOGGER.severe("upper corner 0 : "+ucord[0]);
//		LOGGER.severe("upper corner 1 : "+ucord[1]);
		LOGGER.severe("filter: "+query.getFilter().getClass().getCanonicalName());
		Area a = AggregationUtilities.analyseFilterArea(query.getFilter());
		LOGGER.severe("Parsed filter: "+a.toString());
		return new AggregationFeatureReader( getState(), a);
	}

	//	@Override
	//	protected boolean canLimit() {
	//		return true;
	//	}
	//	@Override
	//	protected boolean canOffset() {
	//		return true;
	//	}
	@Override
	protected boolean canFilter() {
		return true;
	}
	@Override
	//	protected boolean canRetype() {
	//		return true;
	//	}
	//	@Override
	//	protected boolean canSort() {
	//		return true;
	//	}

	protected SimpleFeatureType buildFeatureType() throws IOException {
		SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		builder.setName( entry.getName() );
		AggregationDataStore data = this.getDataStore();
		if(data.hasOutputCount())
			builder.add("cnt", Integer.class);
		if(data.hasOutputSum())
			builder.add("sum", Double.class);
		if(data.hasOutputMin())
			builder.add("min", Double.class);
		if(data.hasOutputMax())
			builder.add("max", Double.class);

		CRSAuthorityFactory   factory = CRS.getAuthorityFactory(true);
		CoordinateReferenceSystem crs = DefaultGeographicCRS.WGS84; // <- Coordinate reference system
		try {
			if(crsNumber==-1)
				crsNumber = this.getDataStore().getCRSNumber(entry);
			crs = factory.createCoordinateReferenceSystem("EPSG:"+crsNumber);
		} catch (NoSuchAuthorityCodeException e) {
			e.printStackTrace();
		} catch (FactoryException e) {
			e.printStackTrace();
		}
		builder.setCRS(crs); // <- Coordinate reference system
		builder.add("area", Polygon.class );

		// build the type (it is immutable and cannot be modified)
		final SimpleFeatureType SCHEMA = builder.buildFeatureType();
		return SCHEMA;
	}

}
