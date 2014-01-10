package org.geotools.data.aggregation;

import java.sql.Timestamp;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.filter.visitor.DefaultFilterVisitor;
import org.opengis.filter.And;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Id;
import org.opengis.filter.IncludeFilter;
import org.opengis.filter.Not;
import org.opengis.filter.Or;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.PropertyIsGreaterThan;
import org.opengis.filter.PropertyIsGreaterThanOrEqualTo;
import org.opengis.filter.PropertyIsLessThan;
import org.opengis.filter.PropertyIsLessThanOrEqualTo;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNil;
import org.opengis.filter.PropertyIsNotEqualTo;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.Add;
import org.opengis.filter.expression.Divide;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.Multiply;
import org.opengis.filter.expression.NilExpression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.expression.Subtract;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.filter.temporal.After;
import org.opengis.filter.temporal.AnyInteracts;
import org.opengis.filter.temporal.Before;
import org.opengis.filter.temporal.Begins;
import org.opengis.filter.temporal.BegunBy;
import org.opengis.filter.temporal.During;
import org.opengis.filter.temporal.EndedBy;
import org.opengis.filter.temporal.Ends;
import org.opengis.filter.temporal.Meets;
import org.opengis.filter.temporal.MetBy;
import org.opengis.filter.temporal.OverlappedBy;
import org.opengis.filter.temporal.TContains;
import org.opengis.filter.temporal.TEquals;
import org.opengis.filter.temporal.TOverlaps;

import com.vividsolutions.jts.geom.Polygon;


public class AggregationFilterVisitor extends DefaultFilterVisitor {
	private static final Logger LOGGER = Logger.getLogger("org.geotools.data.aggregation.AggregationFilterVisitor");
	private Level level = Level.SEVERE;
	//	private boolean valid = true;
	//	private AreaTimeintervalViewparams q = new AreaTimeintervalViewparams();
	private Area area = null;
	//	private HashMap<String,String> viewparams = new HashMap<String,String>();
	private long startTime = -1;
	private long endTime = -1;
	private boolean valid = true;
	private PreAggregate agg;

	public AggregationFilterVisitor(PreAggregate agg){
		super();
		this.agg = agg;
	}

	@Override
	public Object visit(PropertyIsBetween filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		LOGGER.log(level,"Match action: "+filter.getMatchAction().toString());
		filter.getExpression().accept(this, null);
		filter.getLowerBoundary().accept(this, null);
		filter.getUpperBoundary().accept(this, null);
		// if property is TIME then this will be valid
		return null;
	}

	@Override
	public Object visit(BBOX filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		filter.getExpression1().accept(this, null);
		filter.getExpression2().accept(this, null);
		// handle spatial query
		return null;
	}

	@Override
	public Object visit(PropertyName expression, Object data){
		LOGGER.log(level, expression.getClass().getCanonicalName()+": "+expression.getPropertyName());
		if(expression.getPropertyName().equals("area")){
		} else {
			valid = false;
		}
		return super.visit(expression,data);
	}

	@Override
	public Object visit(And filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Literal expression, Object data){
		Object val = expression.getValue();
		LOGGER.log(level, expression.getClass().getCanonicalName()+": "+val.toString());
		if(val instanceof Polygon){
			Polygon poly = (Polygon)val;
			LOGGER.severe(poly.toText());
			Area a = Area.parsePolygon(poly);
			area = updateBounds(area,a);
			LOGGER.severe("low level area: "+a.toString());
		} else if(val instanceof Date){
			LOGGER.severe(val.toString());
			updateTime((Date)val);
		} else {
			LOGGER.log(level,"type of literal val: "+val.getClass().getCanonicalName());
			valid = false;
		}
		return super.visit(expression,data);
	}

	private Area updateBounds(Area a, Area b) {
		if(a==null) return b;
		if(b==null) return a;
		a.updateBounds(b);
		return a;
	}

	private void updateTime(Date t){
		if(startTime==-1)
			startTime = t.getTime();
		else if(endTime==-1){
			if(t.getTime()>startTime)
				endTime = t.getTime();
			else {
				endTime = startTime;
				startTime = t.getTime();
			}
		}else {
			// starTime and endTime are already set
			if(t.getTime()>startTime && t.getTime()<endTime)
				throw new RuntimeException("unexpected update of time in filter parsing");
		}
	}

	@Override
	public Object visit(IncludeFilter filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		area = agg.getArea();
		try{
		long[] timebounds = agg.getTimeBounds();
		startTime = timebounds[0];
		endTime = timebounds[1];
		} catch(NullPointerException e){
			LOGGER.severe("PreAggregate does not contain a time dimension");
		}
		valid = true;
		return super.visit(filter, data);
	}

	// remaining filters are more or less ignored

	@Override
	public Object visit(ExcludeFilter filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Id filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Not filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Or filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(PropertyIsEqualTo filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(PropertyIsNotEqualTo filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(PropertyIsGreaterThan filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(PropertyIsGreaterThanOrEqualTo filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(PropertyIsLessThan filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(PropertyIsLessThanOrEqualTo filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(PropertyIsLike filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(PropertyIsNull filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(PropertyIsNil filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Beyond filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Contains filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Crosses filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Disjoint filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(DWithin filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Equals filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Intersects filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Overlaps filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Touches filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Within filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(After filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(AnyInteracts filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Before filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Begins filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(BegunBy filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(During filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(EndedBy filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Ends filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(Meets filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(MetBy filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(OverlappedBy filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(TContains filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(TEquals filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visit(TOverlaps filter, Object data) {
		LOGGER.log(level, filter.getClass().getCanonicalName());
		valid = false;
		return super.visit(filter, data);
	}

	@Override
	public Object visitNullFilter(Object data) {
		LOGGER.log(level, "NullFilter");
		valid = false;
		return super.visitNullFilter(data);
	}

	// start of expressions 


	@Override
	public Object visit(Add expression, Object data){
		LOGGER.log(level, expression.getClass().getCanonicalName());
		valid = false;
		return super.visit(expression,data);
	}

	@Override
	public Object visit(Multiply expression, Object data){
		LOGGER.log(level, expression.getClass().getCanonicalName());
		valid = false;
		return super.visit(expression,data);
	}

	@Override
	public Object visit(Divide expression, Object data){
		LOGGER.log(level, expression.getClass().getCanonicalName());
		valid = false;
		return super.visit(expression,data);
	}

	@Override
	public Object visit(Subtract expression, Object data){
		LOGGER.log(level, expression.getClass().getCanonicalName());
		valid = false;
		return super.visit(expression,data);
	}

	@Override
	public Object visit(Function expression, Object data){
		LOGGER.log(level, expression.getClass().getCanonicalName());
		valid = false;
		return super.visit(expression,data);
	}

	@Override
	public Object visit(NilExpression expression, Object data){
		LOGGER.log(level, expression.getClass().getCanonicalName());
		valid = false;
		return super.visit(expression,data);
	}

	public Area getArea(){
		return area;
	}

	public Timestamp getStartTime(){
		if(startTime==-1) return null;
		return new Timestamp(startTime);
	}

	public Timestamp getEndTime(){
		if(endTime == -1) return null;
		return new Timestamp(endTime);
	}

	public boolean isValid(){
		return valid;
	}
}
