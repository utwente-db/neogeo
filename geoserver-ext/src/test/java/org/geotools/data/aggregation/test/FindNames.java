package org.geotools.data.aggregation.test;

import java.util.HashSet;
import java.util.Set;

import org.geotools.filter.visitor.DefaultFilterVisitor;
import org.opengis.filter.expression.PropertyName;

public class FindNames extends DefaultFilterVisitor{
	public Set<String> found = new HashSet<String>();

	/** We are only interested in property name expressions */
    public Object visit( PropertyName expression, Object data ) {
        found.add( expression.getPropertyName() );
        return found;
    }
}