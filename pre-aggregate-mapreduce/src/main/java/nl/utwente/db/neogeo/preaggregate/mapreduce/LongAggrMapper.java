package nl.utwente.db.neogeo.preaggregate.mapreduce;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class LongAggrMapper extends AggrMapper<LongAggrWritable> {
    
    public LongAggrMapper () {
        this.aggregateType = AGGR_TYPE.TYPE_BIGINT;
    }

    @Override
    protected void emit(Context context, LongWritable ckey, ResultSet res) throws IOException, SQLException, InterruptedException {
        LongAggrWritable value = new LongAggrWritable(res.getLong("countaggr"), res.getLong("sumaggr"), res.getLong("minaggr"), res.getLong("maxaggr"));
        context.write(ckey, value);
    }
    
}
