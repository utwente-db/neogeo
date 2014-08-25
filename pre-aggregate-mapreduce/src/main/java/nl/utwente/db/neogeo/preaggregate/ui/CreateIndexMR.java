package nl.utwente.db.neogeo.preaggregate.ui;

import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig;
import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig.InvalidConfigException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import nl.utwente.db.neogeo.preaggregate.PreAggregate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import au.com.bytecode.opencsv.CSVReader;
import nl.utwente.db.neogeo.preaggregate.mapreduce.AggrMapper;
import nl.utwente.db.neogeo.preaggregate.mapreduce.IntAggrMapper;
import nl.utwente.db.neogeo.preaggregate.mapreduce.IntAggrReducer;
import nl.utwente.db.neogeo.preaggregate.mapreduce.IntAggrWritable;
import nl.utwente.db.neogeo.preaggregate.mapreduce.WholeFileInputFormat;
import org.apache.hadoop.io.LongWritable;

public class CreateIndexMR {

    static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(AggrMapper.class);
    public static final String HDFS_CONFIG_FILE = "/preaggregate.xml";
    public static final String JAR_PATH = "/jars/";
    private Configuration conf;
    private PreAggregateConfig aggConf;
    private Path inputPath;
    private Path outputPath;
    private File localConfigFile;

    public CreateIndexMR() {
    }

    public void parseArgs(String[] args) throws IOException {
        conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);

        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 3) {
            System.err.println("Usage: <config file> <in> <out>");
            System.exit(2);
        }

        localConfigFile = new File(remainingArgs[0]);
        if (localConfigFile.exists() == false) {
            System.err.println("Config file '" + localConfigFile.getAbsolutePath() + "' does not exist");
            System.exit(2);
        }
        try {
            // test config file
            aggConf = new PreAggregateConfig(localConfigFile);
        } catch (InvalidConfigException ex) {
            throw new IOException("Unable to read PreAggregate config file", ex);
        }

        inputPath = new Path(remainingArgs[1]);
        outputPath = new Path(remainingArgs[2]);
    }

    public boolean runJob() throws IOException, InterruptedException, ClassNotFoundException {
        // this ensures a CSV output (i.e. key is also separated from value with a ,)
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        
        Job job = Job.getInstance(conf, "create pre-aggregate index");
        job.setJarByClass(CreateIndexMR.class);

        job.setInputFormatClass(WholeFileInputFormat.class);

        if (aggConf.getAggregateType().equalsIgnoreCase("int") || aggConf.getAggregateType().equalsIgnoreCase("integer")) {
            job.setMapperClass(IntAggrMapper.class);
            job.setReducerClass(IntAggrReducer.class);

            job.setMapOutputValueClass(IntAggrWritable.class);
            job.setOutputValueClass(IntAggrWritable.class);
        } else {
            throw new UnsupportedOperationException("AggregateType of '" + aggConf.getAggregateType() + "' not yet supported!");
        }

        // re-use Reducer as Combiner
        job.setCombinerClass(job.getReducerClass());
        
        // Set map output and reducer output key Class type (both the ckey -> bigint/long)
        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        
        

        List<String> otherArgs = new ArrayList<String>();

        FileInputFormat.addInputPath(job, inputPath);

        // upload config file to HDFS, overwrite any existing copy. 
        logger.info("Adding config file to DistributedCache...");
        FileSystem fs = FileSystem.get(conf);
        Path hdfsPath = new Path(HDFS_CONFIG_FILE);
        fs.copyFromLocalFile(false, true, new Path(localConfigFile.getAbsolutePath()), hdfsPath);

        job.addCacheFile(hdfsPath.toUri());
        logger.info("File added!");

        // useful for development
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        CreateIndexMR obj = new CreateIndexMR();
        obj.parseArgs(args);
        System.exit(obj.runJob() ? 0 : 1);
    }
}