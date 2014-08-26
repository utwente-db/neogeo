package nl.utwente.db.neogeo.preaggregate.mapreduce;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class DoubleAggrMapper extends AggrMapper<DoubleAggrWritable> {
    
    public DoubleAggrMapper () {
        this.aggregateType = AGGR_TYPE.TYPE_DOUBLE;
    }

    @Override
    protected void emit(Context context, LongWritable ckey, ResultSet res) throws IOException, SQLException, InterruptedException {
        DoubleAggrWritable value = new DoubleAggrWritable(res.getLong("countaggr"), res.getDouble("sumaggr"), res.getDouble("minaggr"), res.getDouble("maxaggr"));
        context.write(ckey, value);
    }
    
}
