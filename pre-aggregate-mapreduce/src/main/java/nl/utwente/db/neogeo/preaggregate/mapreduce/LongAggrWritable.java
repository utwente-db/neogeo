package nl.utwente.db.neogeo.preaggregate.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class LongAggrWritable implements Writable {
    
    private LongWritable countAggr;
    private LongWritable sumAggr;
    private LongWritable minAggr;
    private LongWritable maxAggr;
    
    
    public LongAggrWritable () {
        set(new LongWritable(), new LongWritable(), new LongWritable(), new LongWritable());
    }
    
    public LongAggrWritable (LongWritable countAggr, LongWritable sumAggr, LongWritable minAggr, LongWritable maxAggr) {
        set(countAggr, sumAggr, minAggr, maxAggr);
    }
    
    public LongAggrWritable(long count, long sum, long min, long max) {
        set(new LongWritable(count), new LongWritable(sum), new LongWritable(min), new LongWritable(max));
    }
    
    public void set (LongWritable countAggr, LongWritable sumAggr, LongWritable minAggr, LongWritable maxAggr) {
        this.countAggr = countAggr;
        this.sumAggr = sumAggr;
        this.minAggr = minAggr;
        this.maxAggr = maxAggr;
    }
    
    public long getCount () {
        return this.countAggr.get();
    }
    
    public long getSum() {
        return this.sumAggr.get();
    }
    
    public long getMin () {
        return this.minAggr.get();
    }
    
    public long getMax () {
        return this.maxAggr.get();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        countAggr.readFields(in);
        sumAggr.readFields(in);
        minAggr.readFields(in);
        maxAggr.readFields(in);
        
    }
 
    @Override
    public void write(DataOutput out) throws IOException {
        countAggr.write(out);
        sumAggr.write(out);
        minAggr.write(out);
        maxAggr.write(out);
    }
 
    @Override
    public String toString() {
        return countAggr + "," + sumAggr + "," + minAggr + "," + maxAggr;
    }


    
}
