package org.geotools.data.aggregation;

import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

import org.geotools.filter.LiteralExpression;
import org.opengis.filter.And;
import org.opengis.filter.BinaryComparisonOperator;
import org.opengis.filter.BinaryLogicOperator;
import org.opengis.filter.Filter;
import org.opengis.filter.Or;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.temporal.BinaryTemporalOperator;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

public class AggregationUtilities {
	private static final Logger LOGGER = Logger.getLogger("org.geotools.data.aggregation.AggregationUtilities");

	/**
	 *  And(
	 *  	And(,,),
	 *  		[Lorg.opengis.filter.MultiValuedFilter$MatchAction;@aa4568
	 *  			Expression{org.geotools.filter.AttributeExpressionImpl}-area,
	 *  			Expression{org.geotools.filter.LiteralExpressionImpl}-POLYGON ((-0.1516689453125 51.308060546875005, -0.1516689453125 51.677939453125, 0.4806689453125 51.677939453125, 0.4806689453125 51.308060546875005, -0.1516689453125 51.308060546875005)),)
	 *  		[Lorg.opengis.filter.MultiValuedFilter$MatchAction;@9e9515
	 *  			Expression{org.geotools.filter.AttributeExpressionImpl}-area,
	 *  			Expression{org.geotools.filter.LiteralExpressionImpl}-POLYGON ((-0.1516689453125 51.308060546875005, -0.1516689453125 51.677939453125, 0.4806689453125 51.677939453125, 0.4806689453125 51.308060546875005, -0.1516689453125 51.308060546875005)),
	 * 	    Expression{org.geotools.filter.AttributeExpressionImpl}-area,
	 * 		Expression{org.geotools.filter.LiteralExpressionImpl}-POLYGON ((-0.1516689453125 51.308060546875005, -0.1516689453125 51.677939453125, 0.4806689453125 51.677939453125, 0.4806689453125 51.308060546875005, -0.1516689453125 51.308060546875005)),),)
	 * @param filter
	 * @return
	 */
	public static Area analyseFilterArea(Filter filter) {
		StringBuffer sb = new StringBuffer();
		Area a =  _analyseFilterArea(filter);
		if(a!=null)
			LOGGER.severe("final area: "+a.toString());
		return a;
	}

	/**
	 * depth first traversal of the filter expression
	 * @param todo
	 * @param sb
	 */
	private static Area _analyseFilterArea(Filter cur ) {
		Area a = null;
		List<Filter> childrenF;
		//List<Expression> childrenE = new Vector<Expression>();
		Expression expr1, expr2;
		if(cur instanceof BinaryLogicOperator){
			childrenF = ((BinaryLogicOperator) cur).getChildren();
			if(cur instanceof And){
				for(Filter ff : childrenF){
					a = updateBounds(a, _analyseFilterArea(ff));
				}
			}
		}
		if(cur instanceof BinaryComparisonOperator){
			expr1 = ((BinaryComparisonOperator) cur).getExpression1();
			expr2 = ((BinaryComparisonOperator) cur).getExpression2();
			//			sb.append(((BinaryComparisonOperator) cur).getMatchAction().values());
			//			for(Expression ee : childrenE){
			LOGGER.severe("comparison shortcut: "+((BinaryComparisonOperator) cur).getClass().getCanonicalName());
			a = updateBounds(a, _analyseExpressionArea(expr1,expr2));
			//				sb.append(",");
			//			}
			//			sb.append(")");
		}
		if(cur instanceof BinaryTemporalOperator){
			expr1 = (((BinaryTemporalOperator) cur).getExpression1());
			expr2 = (((BinaryTemporalOperator) cur).getExpression2());
			LOGGER.severe("temporal shortcut: "+((BinaryTemporalOperator) cur).getClass().getCanonicalName());
			a = updateBounds(a, _analyseExpressionArea(expr1,expr2));
			//			sb.append(((BinaryTemporalOperator) cur).getMatchAction().values());
			//			for(Expression ee : childrenE){
			//				a.updateBounds(_analyseExpressionArea(ee));
			//				//				sb.append(",");
			//			}
			//			sb.append(")");
		}
		if(cur instanceof BinarySpatialOperator){
			expr1 = (((BinarySpatialOperator) cur).getExpression1());
			expr2 = (((BinarySpatialOperator) cur).getExpression2());
			LOGGER.severe("spatial shortcut: "+((BinarySpatialOperator) cur).getClass().getCanonicalName());
			a = updateBounds(a, _analyseExpressionArea(expr1, expr2));

			//			sb.append(((BinarySpatialOperator) cur).getMatchAction().values());
			//			for(Expression ee : childrenE){
			//				a.updateBounds(_analyseExpressionArea(ee));
			//				//				sb.append(",");
			//			}
			//			sb.append(")");
		}
//		LOGGER.severe("childrenE :"+(childrenE==null)+"    "+childrenE.size());
//		if(childrenE!=null ){
//			for(int i=0; i<childrenE.size();i++){
//				LOGGER.severe("passed it");
////				a = updateBounds(a, _analyseExpressionArea(childrenE.get(i*2+0),childrenE.get(i*2+1)));
//			}
//		}
		return a;
	}

	private static Area updateBounds(Area a, Area b) {
		if(a==null) return b;
		if(b==null) return a;
		a.updateBounds(b);
		return a;
	}

	private static Area _analyseExpressionArea(Expression ee1, Expression ee2) {
		Area a = null;
		//		sb.append("Expression{"+ee.getClass().getCanonicalName()+"}");
		if(ee1 instanceof PropertyName){
			//			sb.append("-");
			LOGGER.severe("property name:"+((PropertyName)ee1).getPropertyName());
			if(ee2 instanceof Literal && ((PropertyName)ee1).getPropertyName().equals("area")){
				//			sb.append("-")
				Object val = ((Literal)ee2).getValue();
				if(val instanceof Polygon){
					Polygon poly = (Polygon)val;
					LOGGER.severe(poly.toText());
					a = Area.parsePolygon(poly);
					LOGGER.severe("low level area: "+a.toString());
				}
			}
		}
		return a;
	}

	public static String analyseFilter(Filter filter) {
		StringBuffer sb = new StringBuffer();
		_analyseFilter(filter, sb);
		LOGGER.severe("start analysing filter");
		return sb.toString();
	}

	/**
	 * depth first traversal of the filter expression
	 * @param cur
	 * @param sb
	 */
	private static void _analyseFilter(Filter cur, StringBuffer sb ) {
		List<Filter> childrenF;
		List<Expression> childrenE = new Vector<Expression>();
		sb.append(cur.getClass().getCanonicalName());
		sb.append("[");

		if(cur instanceof BinaryLogicOperator){
			childrenF = ((BinaryLogicOperator) cur).getChildren();
			if(cur instanceof And || cur instanceof Or){
				for(Filter ff : childrenF){
					_analyseFilter(ff,sb);
				}
			} else unsupportedFilter(sb, cur);
		}
		if(cur instanceof BinaryComparisonOperator){
			sb.append(((BinaryComparisonOperator) cur).getMatchAction().values());
			childrenE.add(((BinaryComparisonOperator) cur).getExpression1());
			childrenE.add(((BinaryComparisonOperator) cur).getExpression2());
		}
		if(cur instanceof BinaryTemporalOperator){
			sb.append(((BinaryTemporalOperator) cur).getMatchAction().values());
			childrenE.add(((BinaryTemporalOperator) cur).getExpression1());
			childrenE.add(((BinaryTemporalOperator) cur).getExpression2());
		}
		if(cur instanceof BinarySpatialOperator){
			sb.append(((BinarySpatialOperator) cur).getMatchAction().values());
			childrenE.add(((BinarySpatialOperator) cur).getExpression1());
			childrenE.add(((BinarySpatialOperator) cur).getExpression2());
		}
		if(childrenE!=null && childrenE.size()==2)
			_analyseExpression(childrenE.get(0),childrenE.get(1),sb);
		sb.append("]");
	}

	private static void _analyseExpression(Expression ee1, Expression ee2, StringBuffer sb) {
		sb.append("Expression1{"+ee1.getClass().getCanonicalName()+"},");
		if(ee1 instanceof PropertyName){
			sb.append("(");
			sb.append(((PropertyName)ee1).getPropertyName());
			sb.append(")");
		}
		sb.append("-");
		sb.append("Expression2{"+ee2.getClass().getCanonicalName()+"}");
		if(ee2 instanceof Literal ){
			Object val = ((Literal)ee2).getValue();
			sb.append("(");
			sb.append(val);
			sb.append(")");
		}
	}

	private static void unsupportedFilter(StringBuffer sb, Filter ff) {
		sb.append("unsupportedFilter{"+ff.getClass().getCanonicalName()+"}(");
	}
}