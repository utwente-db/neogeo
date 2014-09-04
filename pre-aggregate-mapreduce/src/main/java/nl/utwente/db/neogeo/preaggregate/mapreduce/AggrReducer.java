package nl.utwente.db.neogeo.preaggregate.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class AggrReducer<KEYOUT extends Object, VALUETYPE extends Object> extends Reducer<KEYOUT, VALUETYPE, KEYOUT, VALUETYPE> {
    
    @Override
    protected void reduce(KEYOUT key, Iterable<VALUETYPE> values, Context context) throws IOException, InterruptedException {
        Iterator<VALUETYPE> iter = values.iterator();
        
        if (iter.hasNext() == false) {
            // no values at all? weird situation, just do nothing
            return;            
        }
        
        VALUETYPE first = iter.next();
        
        if (first instanceof IntAggrWritable) {
            reduceInt(key, iter, context, (IntAggrWritable)first);
        } else if (first instanceof LongAggrWritable) {
            reduceLong(key, iter, context, (LongAggrWritable)first);
        } else if (first instanceof DoubleAggrWritable) {
            reduceDouble(key, iter, context, (DoubleAggrWritable)first);
        }
    }
    
    protected void reduceInt (KEYOUT key, Iterator<VALUETYPE> iter, Context context, IntAggrWritable first) throws IOException, InterruptedException {
        long count = first.getCount();
        int sum = first.getSum();
        int min = first.getMin();
        int max = first.getMax();
        
        while(iter.hasNext()) {
            IntAggrWritable row = (IntAggrWritable) iter.next();
            
            count += row.getCount();
            sum += row.getSum();
            
            if (row.getMin() < min) min = row.getMin();
            if (row.getMax() > max) max = row.getMax();           
        }
        
        VALUETYPE newValue = (VALUETYPE) (new IntAggrWritable(count, sum, min, max));
 
        context.write(key, newValue);  
    }
    
    protected void reduceLong (KEYOUT key, Iterator<VALUETYPE> iter, Context context, LongAggrWritable first) throws IOException, InterruptedException {
        long count = first.getCount();
        long sum = first.getSum();
        long min = first.getMin();
        long max = first.getMax();
        
        while(iter.hasNext()) {
            LongAggrWritable row = (LongAggrWritable)iter.next();
            
            count += row.getCount();
            sum += row.getSum();
            
            if (row.getMin() < min) min = row.getMin();
            if (row.getMax() > max) max = row.getMax();           
        }
        
        VALUETYPE newValue = (VALUETYPE) (new LongAggrWritable(count, sum, min, max));
 
        context.write(key, newValue);  
    }
    
    protected void reduceDouble (KEYOUT key, Iterator<VALUETYPE> iter, Context context, DoubleAggrWritable first) throws IOException, InterruptedException {
        long count = first.getCount();
        double sum = first.getSum();
        double min = first.getMin();
        double max = first.getMax();
        
        while(iter.hasNext()) {
            DoubleAggrWritable row = (DoubleAggrWritable)iter.next();
            
            count += row.getCount();
            sum += row.getSum();
            
            if (row.getMin() < min) min = row.getMin();
            if (row.getMax() > max) max = row.getMax();           
        }
        
        VALUETYPE newValue = (VALUETYPE) (new DoubleAggrWritable(count, sum, min, max));
 
        context.write(key, newValue);  
    }
    
}
