package nl.utwente.db.neogeo.preaggregate.ui;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class RunMR {
    protected Configuration conf;
    
    protected FileSystem fs;
    
    public RunMR (Configuration conf) {
        this.conf = conf;
    }
    
    public void setFS (FileSystem fs) {
        this.fs = fs;
    }
    
    public void doJob (String jobPath) {
        
    }
    
}
