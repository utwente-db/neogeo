package nl.utwente.db.neogeo.preaggregate.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class DoubleAggrReducer extends AggrReducer<DoubleAggrWritable> {
    
    @Override
    public void reduce(LongWritable ckey, Iterable<DoubleAggrWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<DoubleAggrWritable> iter = values.iterator();
        
        if (iter.hasNext() == false) {
            // no values at all? weird situation, just do nothing
            return;            
        }
        
        // set initial values
        DoubleAggrWritable first = iter.next();
        long count = first.getCount();
        double sum = first.getSum();
        double min = first.getMin();
        double max = first.getMax();
        
        while(iter.hasNext()) {
            DoubleAggrWritable row = iter.next();
            
            count += row.getCount();
            sum += row.getSum();
            
            if (row.getMin() < min) min = row.getMin();
            if (row.getMax() > max) max = row.getMax();           
        }
        
        DoubleAggrWritable newValue = new DoubleAggrWritable(count, sum, min, max);
 
        context.write(ckey, newValue);
    }
    
}
