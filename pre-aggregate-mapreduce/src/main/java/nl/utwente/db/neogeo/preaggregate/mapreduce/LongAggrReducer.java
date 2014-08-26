package nl.utwente.db.neogeo.preaggregate.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class LongAggrReducer extends AggrReducer<LongAggrWritable> {
    
    @Override
    public void reduce(LongWritable ckey, Iterable<LongAggrWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<LongAggrWritable> iter = values.iterator();
        
        if (iter.hasNext() == false) {
            // no values at all? weird situation, just do nothing
            return;            
        }
        
        // set initial values
        LongAggrWritable first = iter.next();
        long count = first.getCount();
        long sum = first.getSum();
        long min = first.getMin();
        long max = first.getMax();
        
        while(iter.hasNext()) {
            LongAggrWritable row = iter.next();
            
            count += row.getCount();
            sum += row.getSum();
            
            if (row.getMin() < min) min = row.getMin();
            if (row.getMax() > max) max = row.getMax();           
        }
        
        LongAggrWritable newValue = new LongAggrWritable(count, sum, min, max);
 
        context.write(ckey, newValue);
    }
    
}
