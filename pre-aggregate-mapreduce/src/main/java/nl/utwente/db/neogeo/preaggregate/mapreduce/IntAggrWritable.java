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
public class IntAggrWritable implements Writable {
    
    private LongWritable countAggr;
    private IntWritable sumAggr;
    private IntWritable minAggr;
    private IntWritable maxAggr;
    
    
    public IntAggrWritable () {
        set(new LongWritable(), new IntWritable(), new IntWritable(), new IntWritable());
    }
    
    public IntAggrWritable (LongWritable countAggr, IntWritable sumAggr, IntWritable minAggr, IntWritable maxAggr) {
        set(countAggr, sumAggr, minAggr, maxAggr);
    }
    
    public IntAggrWritable(long count, int sum, int min, int max) {
        set(new LongWritable(count), new IntWritable(sum), new IntWritable(min), new IntWritable(max));
    }
    
    public void set (LongWritable countAggr, IntWritable sumAggr, IntWritable minAggr, IntWritable maxAggr) {
        this.countAggr = countAggr;
        this.sumAggr = sumAggr;
        this.minAggr = minAggr;
        this.maxAggr = maxAggr;
    }
    
    public long getCount () {
        return this.countAggr.get();
    }
    
    public int getSum() {
        return this.sumAggr.get();
    }
    
    public int getMin () {
        return this.minAggr.get();
    }
    
    public int getMax () {
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
