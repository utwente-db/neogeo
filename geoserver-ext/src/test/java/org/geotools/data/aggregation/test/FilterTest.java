package org.geotools.data.aggregation.test;

import java.util.Date;
import java.util.HashSet;

import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.visitor.DefaultFilterVisitor;
import org.opengis.feature.FeatureVisitor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;

		
public class FilterTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
	    Filter filter;

	    // the most common selection criteria is a simple equal test
	    ff.equal(ff.property("land_use"), ff.literal("URBAN"));

	    // You can also quickly test if a property has a value
	    filter = ff.isNull(ff.property("approved"));

	    // The usual array of property comparisons is supported
	    // the comparison is based on the kind of data so both
	    // numeric, date and string comparisons are supported.
	    filter = ff.less(ff.property("depth"), ff.literal(300));
	    filter = ff.lessOrEqual(ff.property("risk"), ff.literal(3.7));
	    filter = ff.greater(ff.property("name"), ff.literal("Smith"));
	    filter = ff.greaterOrEqual(ff.property("schedule"), ff.literal(new Date()));

	    // PropertyIsBetween is a short inclusive test between two values
	    filter = ff.between(ff.property("age"), ff.literal(20), ff.literal("29"));
	    filter = ff.between(ff.property("group"), ff.literal("A"), ff.literal("D"));

	    // In a similar fashion there is a short cut for notEqual
	    filter = ff.notEqual(ff.property("type"), ff.literal("draft"));

	    // pattern based "like" filter
	    filter = ff.like(ff.property("code"), "2300%");
	    // you can customise the wildcard characters used
	    filter = ff.like(ff.property("code"), "2300?", "*", "?", "\\");
	    
	    
	    // Pass the visitor to your filter to start the traversal
		FindNames visitor = new FindNames();
		filter.accept( visitor, null );

		System.out.println("Property Names found "+visitor.found );

	}
}

