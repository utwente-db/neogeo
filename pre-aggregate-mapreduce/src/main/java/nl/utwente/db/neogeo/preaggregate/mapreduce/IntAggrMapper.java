/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.neogeo.preaggregate.mapreduce;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author
 * Dennis
 * Pallett
 * <dennis@pallett.nl>
 */
public class IntAggrMapper extends AggrMapper<IntAggrWritable> {
    
    public IntAggrMapper () {
        this.aggregateType = AGGR_TYPE.TYPE_INT;
    }

    @Override
    protected void emit(Context context, LongWritable ckey, ResultSet res) throws IOException, SQLException, InterruptedException {
        IntAggrWritable value = new IntAggrWritable(res.getInt("countaggr"), res.getInt("sumaggr"), res.getInt("minaggr"), res.getInt("maxaggr"));
        context.write(ckey, value);
    }
    
}
