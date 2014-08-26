package nl.utwente.db.neogeo.preaggregate.ui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import nl.utwente.db.neogeo.preaggregate.SqlUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 *
 * @author Dennis Pallett <dennis@pallett.nl>
 */
public class FinishMRPostgres extends FinishMR {
    static final Logger logger = Logger.getLogger(FinishMRPostgres.class);
    
    protected CopyManager cm;
    
    protected CopyIn copyIn;
        
    public FinishMRPostgres (Configuration conf, DbInfo dbInfo, Connection c) {
        super(conf, dbInfo, c);
    }
    
    @Override
    protected void prepareInsertData (String indexTable) throws IOException, FinishException {
        try {
            cm = new CopyManager((BaseConnection) c);
            
            String copyQuery = "COPY " + indexTable + " FROM STDIN WITH CSV DELIMITER ',';";

            copyIn = cm.copyIn(copyQuery);
        } catch (SQLException ex) {
            throw new FinishException("Unable to initialize PostgreSQL CopyManager", ex);
        }
    }
        
    @Override
    protected void finishInsertData () throws IOException, FinishException {
        try {
            copyIn.endCopy();
        } catch (SQLException ex) {
            throw new FinishException("Unable to finish COPY operation", ex);
        }
    }
    
    @Override
    protected void insertDataIntoTable (InputStream in) throws IOException, SQLException {    
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        
        // get input stream as byte array
        byte[] buff = IOUtils.toByteArray(in);
        
        // copy to table
        copyIn.writeToCopy(buff, 0, buff.length);
    }
}
