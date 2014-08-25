/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.utwente.db.neogeo.preaggregate.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author
 * Dennis
 * Pallett
 * <dennis@pallett.nl>
 */
public abstract class AggrReducer<VALUETYPE extends Object> extends Reducer<LongWritable, VALUETYPE, LongWritable, VALUETYPE> {
    
}
