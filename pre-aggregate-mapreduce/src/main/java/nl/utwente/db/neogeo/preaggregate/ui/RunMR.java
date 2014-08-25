package nl.utwente.db.neogeo.preaggregate.ui;

import java.io.IOException;
import java.util.Scanner;
import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig;
import nl.utwente.db.neogeo.preaggregate.mapreduce.IntAggrMapper;
import nl.utwente.db.neogeo.preaggregate.mapreduce.IntAggrReducer;
import nl.utwente.db.neogeo.preaggregate.mapreduce.IntAggrWritable;
import nl.utwente.db.neogeo.preaggregate.mapreduce.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class RunMR {
    static final Logger logger = Logger.getLogger(RunMR.class);

    public static final String CONFIG_FILENAME = "preaggregate.xml";
    
    public static final String LFP_TABLE_FILENAME = "lfp_table.csv";
    
    protected Configuration conf;
    
    protected PreAggregateConfig config;
    
    protected FileSystem fs;
    
    public RunMR (Configuration conf, PreAggregateConfig config) {
        this.conf = conf;
        this.config = config;
    }
    
    public void setFS (FileSystem fs) {
        this.fs = fs;
    }
    
    public long doJob (String jobPath) throws IOException, RunException, ClassNotFoundException, InterruptedException {
        long startTime = System.currentTimeMillis();
        
        // verify paths exist
        if (fs.exists(new Path(jobPath)) == false) {
            throw new RunException("Job path does not exist on HDFS");
        }
        
        Path configPath = new Path(jobPath + "/" + CONFIG_FILENAME);
        if (fs.exists(configPath) == false) {
            throw new RunException("Missing PreAggregateConfig XML file on HDFS");
        }
        
        Path lfpTablePath = new Path(jobPath + "/" + LFP_TABLE_FILENAME);
        if (fs.exists(lfpTablePath) == false) {
            throw new RunException("Missing LFP table CSV file on HDFS");
        }
        
        // check if jobPath already exists on HDFS
        if (fs.exists(new Path(jobPath + "/output"))) {
            long pauseStartTime = System.currentTimeMillis();
            
            logger.warn("Output directory already exists on HDFS. Delete existing directory? (yes|no)");
            Scanner scan = new Scanner(System.in);
            String s = scan.next().toLowerCase();
            
            while(s.startsWith("y") == false && s.startsWith("n") == false) {
                logger.warn("Delete existing directory? (yes|no)");
                s = scan.next().toLowerCase();
            }
            
            if (s.toLowerCase().startsWith("n")) {
                logger.error("Quiting!");
                System.exit(0);
            }
            
            // increment start time with time paused
            long pauseTime = System.currentTimeMillis() - pauseStartTime;
            startTime += pauseTime;
            
            // delete directory
            logger.info("Deleting existing output directory...");
            fs.delete(new Path(jobPath + "/output"), true);
            logger.info("Directory deleted");
        }
        
        // this ensures a CSV output (i.e. key is also separated from value with a ,)
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        
        Job job = Job.getInstance(conf, "Create PreAggregate Index");
        job.setJarByClass(RunMR.class);
        
        job.setInputFormatClass(WholeFileInputFormat.class);
        
        if (config.getAggregateType().equalsIgnoreCase("int") || config.getAggregateType().equalsIgnoreCase("integer")) {
            job.setMapperClass(IntAggrMapper.class);
            job.setReducerClass(IntAggrReducer.class);

            job.setMapOutputValueClass(IntAggrWritable.class);
            job.setOutputValueClass(IntAggrWritable.class);
        } else {
            throw new UnsupportedOperationException("AggregateType of '" + config.getAggregateType() + "' not yet supported!");
        }
        
        // re-use Reducer as Combiner
        job.setCombinerClass(job.getReducerClass());
        
        // Set map output and reducer output key Class type (both the ckey -> bigint/long)
        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        
        // add helper files to DistributedCache
        job.addCacheFile(configPath.toUri());
        job.addCacheFile(lfpTablePath.toUri());
        
        FileInputFormat.addInputPath(job, new Path(jobPath + "/input"));        
        FileOutputFormat.setOutputPath(job, new Path(jobPath + "/output"));
                
        job.waitForCompletion(true);        
        
        long execTime = System.currentTimeMillis() - startTime;
        return execTime;
    }
    
    class RunException extends Exception {
        public RunException (String msg) {
            super(msg);
        }
        
        public RunException (String msg, Exception ex) {
            super(msg, ex);
        }
    }
    
}
