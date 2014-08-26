package nl.utwente.db.neogeo.preaggregate.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class DoubleAggrWritable implements Writable {
    
    private LongWritable countAggr;
    private DoubleWritable sumAggr;
    private DoubleWritable minAggr;
    private DoubleWritable maxAggr;
    
    
    public DoubleAggrWritable () {
        set(new LongWritable(), new DoubleWritable(), new DoubleWritable(), new DoubleWritable());
    }
    
    public DoubleAggrWritable (LongWritable countAggr, DoubleWritable sumAggr, DoubleWritable minAggr, DoubleWritable maxAggr) {
        set(countAggr, sumAggr, minAggr, maxAggr);
    }
    
    public DoubleAggrWritable(long count, double sum, double min, double max) {
        set(new LongWritable(count), new DoubleWritable(sum), new DoubleWritable(min), new DoubleWritable(max));
    }
    
    public void set (LongWritable countAggr, DoubleWritable sumAggr, DoubleWritable minAggr, DoubleWritable maxAggr) {
        this.countAggr = countAggr;
        this.sumAggr = sumAggr;
        this.minAggr = minAggr;
        this.maxAggr = maxAggr;
    }
    
    public long getCount () {
        return this.countAggr.get();
    }
    
    public double getSum() {
        return this.sumAggr.get();
    }
    
    public double getMin () {
        return this.minAggr.get();
    }
    
    public double getMax () {
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
