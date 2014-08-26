package nl.utwente.db.neogeo.preaggregate.ui;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import nl.cwi.monetdb.mcl.MCLException;
import nl.cwi.monetdb.mcl.io.BufferedMCLReader;
import nl.cwi.monetdb.mcl.io.BufferedMCLWriter;
import nl.cwi.monetdb.mcl.net.MapiSocket;
import nl.cwi.monetdb.mcl.parser.MCLParseException;
import nl.utwente.db.neogeo.preaggregate.PreAggregateConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class PrepareMRPostgres extends PrepareMR {
    static final Logger logger = Logger.getLogger(PrepareMRPostgres.class);
    
    protected CopyManager cm;

    public PrepareMRPostgres (Configuration conf, DbInfo dbInfo, Connection c, PreAggregateConfig config) {
        super(conf, dbInfo, c, config);
    }
    
    @Override
    protected void prepareCreateChunks () throws PrepareException, IOException {
        try {
            cm = new CopyManager((BaseConnection) c);
        } catch (SQLException ex) {
            throw new PrepareException("Unable to initialize PostgreSQL CopyManager", ex);
        }
    }
    
    @Override
    protected int writeChunk(StringBuilder where, String fileName, int chunkNum) throws IOException, SQLException {
        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempPath.getAbsolutePath() + fileName), "utf-8"));
        
        String chunkSelectQuery = this.select_level0(c, table, where.toString(), axis, aggregateColumn, aggregateMask);
        
        String copyQuery = "COPY (" + chunkSelectQuery + ") TO STDOUT WITH CSV DELIMITER ',' QUOTE '\"' ENCODING 'UTF-8' FORCE QUOTE *";
        
        // do Copy
        int ret = (int)cm.copyOut(copyQuery, writer);
                        
        // finalize chunk
        writer.close();       
        
        return ret;
    }
    
    @Override
    protected void finishCreateChunks () throws PrepareException, IOException {
        cm = null;
    }
    
}
