/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.neogeo.preaggregate.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author
 * Dennis
 * Pallett
 * <dennis@pallett.nl>
 */
public class IntAggrReducer extends AggrReducer<IntAggrWritable> {
    
    @Override
    public void reduce(LongWritable ckey, Iterable<IntAggrWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntAggrWritable> iter = values.iterator();
        
        if (iter.hasNext() == false) {
            // no values at all? weird situation, just do nothing
            return;            
        }
        
        // set initial values
        IntAggrWritable first = iter.next();
        long count = first.getCount();
        int sum = first.getSum();
        int min = first.getMin();
        int max = first.getMax();
        
        while(iter.hasNext()) {
            IntAggrWritable row = iter.next();
            
            count += row.getCount();
            sum += row.getSum();
            
            if (row.getMin() < min) min = row.getMin();
            if (row.getMax() > max) max = row.getMax();           
        }
        
        IntAggrWritable newValue = new IntAggrWritable(count, sum, min, max);
 
        context.write(ckey, newValue);
    }
    
}
